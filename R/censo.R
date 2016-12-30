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
		files_to_download_2010 <- all_files[ !( all_files %in% c( '1_Atualizacoes_20160311.txt' , 'Atualizacoes.txt' , 'Documentacao.zip' ) ) ]

		catalog <-
			rbind( 
				catalog ,
				data.frame(
					year = 2010 ,
					db_table_prefix = gsub( ".zip" , "" , files_to_download_2010 , ignore.case = TRUE ) ,
					dbfolder = paste0( output_dir , "/MonetDB" ) ,
					pes_design = paste0( output_dir , "/pes 2010 design.rda" ) ,
					pes_sas = system.file("extdata", "SASinputPes.txt", package = "lodown") ,
					dom_design = paste0( output_dir , "/dom 2010 design.rda" ) ,
					dom_sas = system.file("extdata", "SASinputDom.txt", package = "lodown") ,
					fam_design = NA ,
					fam_sas = NA ,
					dom_ranc = NA ,
					pes_ranc = NA ,
					fam_ranc = NA ,
					stringsAsFactors = FALSE
				)
			)

			
		# designate the location of the 2000 general sample microdata files
		ftp_path_2000 <-	"ftp://ftp.ibge.gov.br/Censos/Censo_Demografico_2000/Microdados/"

		# fetch all available files in the ftp site's directory
		all_files <- RCurl::getURL( ftp_path_2000 , dirlistonly = TRUE )

		# those files are separated by newline characters in the code,
		# so simply split them up into a character vector
		# full of individual zipped file strings
		all_files <- scan( text = all_files , what = "character", quiet = T )

		# remove the two files you don't need to import
		files_to_download_2000 <- all_files[ !( all_files %in% c( '1_Documentacao.zip' , '2_Atualizacoes.txt' , '1_Documentacao_velho.zip' , "2_Atualizacoes_20160309.txt" , "1_Documentacao_20160309.zip" ) ) ]

		catalog <-
			rbind( 
				catalog ,
				data.frame(
					year = 2000 ,
					db_table_prefix = gsub( ".zip" , "" , files_to_download_2000 , ignore.case = TRUE ) ,
					dbfolder = paste0( output_dir , "/MonetDB" ) ,
					pes_design = paste0( output_dir , "/pes 2000 design.rda" ) ,
					pes_sas = system.file("extdata", "LE_PESSOAS.sas", package = "lodown") ,
					dom_design = paste0( output_dir , "/dom 2000 design.rda" ) ,
					dom_sas = system.file("extdata", "LE_DOMIC.sas", package = "lodown") ,
					fam_design = paste0( output_dir , "/fam 2000 design.rda" ) ,
					fam_sas = system.file("extdata", "LE_FAMILIAS.sas", package = "lodown") ,
					dom_ranc = 170 ,
					pes_ranc = 390 ,
					fam_ranc = 118 ,
					stringsAsFactors = FALSE
				)
			)
		
		catalog
	
	}

lodown_censo <-
	function( data_name = "censo" , catalog , path_to_7za = '7za' , ... ){

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			# open the connection to the monetdblite database
			db <- DBI::dbConnect( MonetDBLite::MonetDBLite() , catalog[ i , 'dbfolder' ] )

			unzipped_files <- cachaca( this_download , tf , mode = 'wb' )
					
			dom_file <- unzipped.files[ grep( 'DOM|Domicilios' , unzipped.files , useBytes = TRUE ) ]
			pes_file <- unzipped.files[ grep( 'PES|Pessoas' , unzipped.files , useBytes = TRUE ) ]
			fam_file <- unzipped.files[ grep( 'FAM' , toupper( unzipped.files ) , useBytes = TRUE ) ]

			this_dom <- ifelse( !is.na( catalog[ i , 'dom_ranc' ] ) , ranc( dom_file , catalog[ i , 'dom_ranc' ] ) , dom_file )
			this_pes <- ifelse( !is.na( catalog[ i , 'pes_ranc' ] ) , ranc( pes_file , catalog[ i , 'pes_ranc' ] ) , pes_file )
			this_fam <- ifelse( !is.na( catalog[ i , 'fam_ranc' ] ) , ranc( fam_file , catalog[ i , 'fam_ranc' ] ) , fam_file )
			
			
			for( this_dom in dom_file ){
			
				read_SAScii_monetdb (
					this_dom ,
					sas_ri = catalog[ i , 'dom_sas' ] ,
					zipped = FALSE ,
					tl = TRUE ,
					tablename = paste0( catalog[ i , 'db_table_prefix' ] , '_dom' ) ,
					connection = db
				)

			}
			
			
			for( this_pes in pes_file ){
			
				read_SAScii_monetdb (
					this_pes ,
					sas_ri = catalog[ i , 'pes_sas' ] ,
					zipped = FALSE ,
					tl = TRUE ,
					tablename = paste0( catalog[ i , 'db_table_prefix' ] , '_pes' ) ,
					connection = db
				)

			}
			
			for( this_fam in fam_file ){
			
				read_SAScii_monetdb (
					this_fam ,
					sas_ri = catalog[ i , 'fam_sas' ] ,
					zipped = FALSE ,
					tl = TRUE ,
					tablename = paste0( catalog[ i , 'db_table_prefix' ] , '_fam' ) ,
					connection = db
				)

			}
			
			
			
			
			# disconnect from the current monet database
			DBI::dbDisconnect( db , shutdown = TRUE )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'db_tablename' ] , "'\r\n\n" ) )

		}
		
		
		invisible( TRUE )

	}

	

# define a special function to   #
# remove alphanumeric characters #
# from any data files that have  #
# been downloaded from ibge      #
ranc_censo <- 
	function( infiles , width ){

		tf_a <- tempfile()

		outcon <- file( tf_a , "w" )

		# if there are multiple infiles,
		# loop through them all!
		for ( infile in infiles ){

			incon <- file( infile , "r")

			line.num <- 0
			
			while( length( line <- readLines( incon , 1 , skipNul = TRUE ) ) > 0 ){

				# add blank spaces on the right side where they're absent.
				line <- stringr::str_pad( line , width , side = "right" , pad = " " )
				
				# save the file on the disk, so long as there's no weird corruption
				if( !is.na( iconv( line , "" , "ASCII" ) ) ) writeLines( line , outcon )
				# like line 509,451 and 2,575,789 of the combined 2000 sao paulo file.

				# add to the line counter #
				line.num <- line.num + 1

			}

			close( incon )
		}

		close( outcon )

		tf_a
	}
