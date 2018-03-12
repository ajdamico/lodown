get_catalog_censo <-
	function( data_name = "censo" , output_dir , ... ){

		catalog <- NULL

		# designate the location of the 2010 general sample microdata files
		ftp_path_2010 <-	"ftp://ftp.ibge.gov.br/Censos/Censo_Demografico_2010/Resultados_Gerais_da_Amostra/Microdados/"

		# fetch all available files in the ftp site's directory
		all_files <- RCurl::getURL( ftp_path_2010 , dirlistonly = TRUE )

		# those files are separated by newline characters in the code,
		# so simply split them up into a character vector
		# full of individual zipped file strings
		all_files <- scan( text = all_files , what = "character", quiet = T )

		# remove the two files you don't need to import
		files_to_download_2010 <- all_files[ !grepl( "documentacao|atualizacoes" , all_files , ignore.case = TRUE ) ]

		catalog <-
			rbind(
				catalog ,
				data.frame(
					full_url = paste0( ftp_path_2010 , files_to_download_2010 ) ,
					year = 2010 ,
					state = tolower( gsub( ".zip" , "10" , files_to_download_2010 , ignore.case = TRUE ) ) ,
					output_folder = file.path( output_dir , "2010" ) ,
					pes_sas = censo_sas( system.file("extdata", "censo/SASinputPes.txt", package = "lodown") ) ,
					dom_sas = censo_sas( system.file("extdata", "censo/SASinputDom.txt", package = "lodown") ) ,
					fam_sas = NA ,
					fpc1 = 'v0011' ,
					fpc2 = 'v0010' ,
					fpc3 = 'v0001' ,
					fpc4 = 'v0300' ,
					fpc5 = NA ,
					weight = 'v0010' ,
					stringsAsFactors = FALSE
				)
			)


		# designate the location of the 2000 general sample microdata files
		ftp_path_2000 <- "ftp://ftp.ibge.gov.br/Censos/Censo_Demografico_2000/Microdados/"

		# fetch all available files in the ftp site's directory
		all_files <- RCurl::getURL( ftp_path_2000 , dirlistonly = TRUE )

		# those files are separated by newline characters in the code,
		# so simply split them up into a character vector
		# full of individual zipped file strings
		all_files <- scan( text = all_files , what = "character", quiet = T )

		# remove the two files you don't need to import
		files_to_download_2000 <- all_files[ !grepl( "documentacao|atualizacoes" , all_files , ignore.case = TRUE ) ]

		catalog <-
			rbind(
				catalog ,
				data.frame(
					full_url = paste0( ftp_path_2000 , files_to_download_2000 ) ,
					year = 2000 ,
					state = tolower( gsub( ".zip" , "00" , files_to_download_2000 , ignore.case = TRUE ) ) ,
					output_folder = file.path( output_dir , "2000" ) ,
					pes_sas = censo_sas( system.file("extdata", "censo/LE_PESSOAS.sas", package = "lodown") ) ,
					dom_sas = censo_sas( system.file("extdata", "censo/LE_DOMIC.sas", package = "lodown") ) ,
					fam_sas = censo_sas( system.file("extdata", "censo/LE_FAMILIAS.sas", package = "lodown") ) ,
					fpc1 = "areap" ,
					fpc2 = "p001" ,
					fpc3 = 'v0102' ,
					fpc4 = 'v0300' ,
					fpc5 = 'v0404' ,
					weight = 'p001' ,
					stringsAsFactors = FALSE
				)
			)

		catalog[ order( catalog$year , catalog$state ) , ]

	}

lodown_censo <-
	function( data_name = "censo" , catalog , ... ){

		on.exit( print( catalog ) )

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			cachaca( catalog[ i , 'full_url' ] , tf , mode = 'wb' )

			unzipped_files <- unzip_warn_fail( tf , exdir = catalog[ i , 'output_folder' ] )
			
			# some zipped files contained zipped subfiles
			for( this_zip in grep( "\\.zip$" , unzipped_files , ignore.case = TRUE , value = TRUE ) ){
			
				unzipped_files <- unzipped_files[ unzipped_files != this_zip ]
				
				unzipped_files <- c( unzipped_files , unzip_warn_fail( this_zip , exdir = catalog[ i , 'output_folder' ] ) )
				
			}
			
			dom_file <- unzipped_files[ grep( 'DOM' , unzipped_files , useBytes = TRUE , ignore.case = TRUE ) ]
			pes_file <- unzipped_files[ grep( 'PES' , unzipped_files , useBytes = TRUE , ignore.case = TRUE ) ]
			fam_file <- unzipped_files[ grep( 'FAM' , toupper( unzipped_files ) , useBytes = TRUE , ignore.case = TRUE ) ]

			# the 2000 rn state file contains multiple files..  stack them manually
			if( catalog[ i , 'year' ] == 2000 & catalog[ i , 'state' ] == 'rn00' ){
			
				these_lines <- unlist( lapply( dom_file , readLines ) )
				dom_file <- dom_file[ 1 ]
				writeLines( these_lines , dom_file ) ; rm( these_lines ) ; gc()
				
				these_lines <- unlist( lapply( fam_file , readLines ) )
				fam_file <- fam_file[ 1 ]
				writeLines( these_lines , fam_file ) ; rm( these_lines ) ; gc()
				
				these_lines <- unlist( lapply( pes_file , readLines ) )
				pes_file <- pes_file[ 1 ]
				writeLines( these_lines , pes_file ) ; rm( these_lines ) ; gc()
				
			}
			
			stopifnot( length( dom_file ) < 2 )
			stopifnot( length( pes_file ) < 2 )
			stopifnot( length( fam_file ) < 2 )

			catalog[ i , 'dom_file' ] <- dom_file
			catalog[ i , 'pes_file' ] <- pes_file
			catalog[ i , 'fam_file' ] <- if( length( fam_file ) == 0 ) NA else fam_file				
			
			# add the number of records to the catalog
			catalog[ i , 'case_count' ] <- R.utils::countLines( pes_file )

			# remove extracted files and tf
			file.remove( tf )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'output_folder' ] , "'\r\n\n" ) )

		}

		on.exit()
		
		catalog

	}


censo_sas <-
	function( sasfile ){

		tf <- tempfile()

		incon <- file( sasfile , "r" , encoding = "windows-1252" )

		this_sas <- readLines( incon )

		writeLines( iconv( this_sas , "" , "ASCII//TRANSLIT" , sub = " " ) , tf )

		tf
	}
