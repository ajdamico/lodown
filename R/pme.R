get_catalog_pme <-
	function( data_name = "pme" , output_dir , ... ){

		# read the text of the microdata ftp into working memory
		# download the contents of the ftp directory for all microdata
		ftp.listing <- readLines( textConnection( RCurl::getURL( "ftp://ftp.ibge.gov.br/Trabalho_e_Rendimento/Pesquisa_Mensal_de_Emprego/Microdados/" ) ) )

		# extract the text from all lines containing a year of microdata
		# figure out the names of those year directories
		ay <- rev( gsub( "(.*) (.*)" , "\\2" , ftp.listing ) )

		# remove non-numeric strings
		suppressWarnings( available_years <- ay[ as.numeric( ay ) %in% ay ] )
		# now `available.years` should contain all of the available years on the pme ftp site

		all_zipfiles <- all_ym <- NULL
		
		for( this_year in available_years ){
		
			# define path of this year
			year_dir <- paste0( "ftp://ftp.ibge.gov.br/Trabalho_e_Rendimento/Pesquisa_Mensal_de_Emprego/Microdados/" , this_year , "/" )

			cat( paste0( "loading " , data_name , " catalog from " , year_dir , "\r\n\n" ) )

			# just like above, read those lines into working memory
			year.ftp.string <- readLines( textConnection( RCurl::getURL( year_dir ) ) )
			
			# break up the string based on the ending extension
			zip.lines <- unlist( strsplit( year.ftp.string , "\\.zip$" ) )
			
			# extract the precise filename of the `.zip` file
			zip.filenames <- gsub( '(.*) (.*)' , "\\2.zip" , zip.lines )

			# in 2008, the files are named by three-letter month.
			# in portuguese, sorted alphabetically, april is the first month, followed by august, and so on.
			if ( this_year == 2008 ){

				available.months <- c( '04' , '08' , '12' , '02' , '01' , '07' , '06' , '05' , '03' , '11' , '10' , '09' )
			
			} else {

				# for all zip file names,
				# find the pattern starting with `PMEnova`
				# and ending with the year x month dot zip.
				available.months <- gsub( "(PMEnova)(.)([0-9][0-9])([0-9][0-9][0-9][0-9])([0-9]?)(.zip)" , "\\3" , zip.filenames )
			
			}

			# construct the full ftp path to the current zipped file
			all_zipfiles <-
				c( 
					all_zipfiles ,
					paste0(
						"ftp://ftp.ibge.gov.br/Trabalho_e_Rendimento/Pesquisa_Mensal_de_Emprego/Microdados/" , 
						this_year ,
						"/" ,
						zip.filenames
					)	
				)
				
			all_ym <- c( all_ym , paste( this_year , available.months ) )
				
		}
		
		
		catalog <-
			data.frame(
				year = substr( all_ym , 1 , 4 ) ,
				month = substr( all_ym , 6 , 7 ) ,
				full_url = all_zipfiles ,
				output_filename = paste0( output_dir , "/" , 'pme ' , all_ym , '.rds' ) ,
				stringsAsFactors = FALSE
			)

		catalog

	}


lodown_pme <-
	function( data_name = "pme" , catalog , ... ){

		on.exit( print( catalog ) )

		tf <- tempfile()

		cachaca( "ftp://ftp.ibge.gov.br/Trabalho_e_Rendimento/Pesquisa_Mensal_de_Emprego/Microdados/documentacao/Documentacao.zip" , tf )

		unzipped_files <- unzip_warn_fail( tf , exdir = unique( np_dirname( catalog$output_filename ) )  )

		# hold onto only the filename containing the word `INPUT`
		input <- unzipped_files[ grep( "INPUT" , unzipped_files ) ]
	
		for ( i in seq_len( nrow( catalog ) ) ){

			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

			unzipped_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , '/unzips' ) )

			# ..and read that text file directly into an R data.frame
			# using the sas importation script downloaded before this big fat loop
			x <- read_SAScii( unzipped_files , input )
			
			# convert all column names to lowercase
			names( x ) <- tolower( names( x ) )
			
			catalog[ i , 'case_count' ] <- nrow( x )
			
			saveRDS( x , file = catalog[ i , 'output_filename' ] , compress = FALSE )

			# delete the temporary files
			file.remove( tf , unzipped_files )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

		}

		on.exit()
		
		catalog

	}

