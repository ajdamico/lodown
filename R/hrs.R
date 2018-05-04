get_catalog_hrs <-
	function( data_name = "hrs" , output_dir , ... ){

		catalog <- NULL
	
		if( !( 'your_username' %in% names(list(...)) ) ) stop( "`your_username` parameter must be specified.  create an account at https://ssl.isr.umich.edu/hrs/reg_pub2.php" )

		if( !( 'your_password' %in% names(list(...)) ) ) stop( "`your_password` parameter must be specified.  create an account at https://ssl.isr.umich.edu/hrs/reg_pub2.php" )
		
		your_username <- list(...)[["your_username"]]
						
		your_password <- list(...)[["your_password"]]
		
		# authentication page
		terms <- "https://ssl.isr.umich.edu/hrs/login2.php"

		# download page
		download <- "https://ssl.isr.umich.edu/hrs/files2.php"


		# set the username and password
		values <- list( fuser = your_username , fpass = your_password )

		# accept the terms on the form, 
		# generating the appropriate cookies
		httr::POST( terms , body = values )

		# download the content of that download page
		resp <- httr::GET( download , query = values )

		all_links <- rvest::html_nodes( httr::content( resp ) , "a" )
		
		link_text <- rvest::html_text( all_links )
		
		link_refs <- rvest::html_attr( all_links , "href" )
		
		stopifnot( length( link_text ) == length( link_refs ) )
		
		valid_versids <- paste0( "https://ssl.isr.umich.edu/hrs/" , link_refs[ grepl( 'versid' , link_refs ) ] )
		
		versid_text <- stringr::str_trim( link_text[ grepl( 'versid' , link_refs ) ] )
		
		for( this_page in seq_along( valid_versids ) ){
		
			this_resp <- httr::GET( valid_versids[ this_page ] , query = values )

			all_links <- rvest::html_nodes( httr::content( this_resp , encoding = 'latin1' ) , "a" )
			
			link_text <- rvest::html_text( all_links )
			
			link_refs <- rvest::html_attr( all_links , "href" )
			
			stopifnot( length( link_text ) == length( link_refs ) )
			
			these_versids <- paste0( "https://ssl.isr.umich.edu/hrs/" , link_refs[ grepl( 'filedownload2\\.php\\?d' , link_refs ) ] )
			
			this_text <- stringr::str_trim( link_text[ grepl( 'filedownload2\\.php\\?d' , link_refs ) ] )

			if( length( this_text ) > 0 ){
				
				this_cat <-
					data.frame(
						file_title = versid_text[ this_page ] ,
						file_name = this_text ,
						full_url = these_versids ,
						stringsAsFactors = FALSE
					)
					
			} else this_cat <- data.frame( NULL )
		
			if( nrow( this_cat ) > 0 ){
			
				this_cat$year <- ifelse( grepl( "^[0-9][0-9][0-9][0-9]" , this_cat$file_title ) , substr( this_cat$file_title , 1 , 4 ) , NA )
			
				this_cat$output_filename <- paste0( output_dir , "/" , ifelse( is.na( this_cat$year ) , "" , paste0( this_cat$year , "/" ) ) , this_cat$file_name )
						
				if( !any( grepl( "rand" , this_cat$output_filename , ignore.case = TRUE ) ) & !any( grepl( "rand" , this_cat$file_title , ignore.case = TRUE ) ) ){

					this_table <- rvest::html_table( httr::content( this_resp , encoding = 'latin1' ) , fill = TRUE )
					
					which_table <- which( unlist( lapply( this_table , function( z ) any( grepl( "distribution set" , z , ignore.case = TRUE ) ) ) ) )
				
					if( length( which_table ) == 1 ){
				
						which_column <- sapply( this_table[[ which_table ]] , function( z ) any( grepl( "distribution set" , z , ignore.case = TRUE ) ) )
					
						which_file <- grep( "distribution set" , this_table[[ which_table ]][ , which_column ] , ignore.case = TRUE )
						
						this_cat[ which_file , 'output_folder' ] <- gsub( "\\.zip" , "" , this_cat[ which_file , 'output_filename' ] , ignore.case = TRUE )
					
					} else this_cat$output_folder <- NA
					
				} else {
				
					this_cat$output_folder <- ifelse( grepl( "sta\\.zip$|stata\\.zip$" , this_cat$output_filename , ignore.case = TRUE ) , gsub( "\\.zip" , "" , this_cat$output_filename , ignore.case = TRUE ) , NA )

				}

			
				this_cat <- this_cat[ grepl( "\\.zip$" , this_cat$file_name , ignore.case = TRUE ) , ]
				
				catalog <- rbind( catalog , this_cat )
			
			}
			
		}
		
		catalog <- catalog[ !is.na( catalog$output_folder ) , ]
		
		# skip the corrupted zip harmonized hrs version A
		catalog <- subset( catalog , !grepl( "HarmonizedHRSA\\.zip$" , output_filename ) )
		
		catalog
		
	}
						
		
lodown_hrs <-
	function( data_name = "hrs" , catalog , ... ){

		on.exit( print( catalog ) )

		tf <- tempfile()

		if( !( 'your_username' %in% names(list(...)) ) ) stop( "`your_username` parameter must be specified.  create an account at https://ssl.isr.umich.edu/hrs/reg_pub2.php" )

		if( !( 'your_password' %in% names(list(...)) ) ) stop( "`your_password` parameter must be specified.  create an account at https://ssl.isr.umich.edu/hrs/reg_pub2.php" )
		
		your_username <- list(...)[["your_username"]]
						
		your_password <- list(...)[["your_password"]]
		
		# authentication page
		terms <- "https://ssl.isr.umich.edu/hrs/login2.php"

		# download page
		download <- "https://ssl.isr.umich.edu/hrs/files2.php"

		# set the username and password
		values <- list( fuser = your_username , fpass = your_password )

		# accept the terms on the form, 
		# generating the appropriate cookies
		httr::POST( terms , body = values )

		for ( i in seq_len( nrow( catalog ) ) ){

			# download the file
			this_file <- cachaca( catalog[ i , "full_url" ] , FUN = httr::GET , filesize_fun = 'unzip_verify' )

			writeBin( httr::content( this_file , "raw" ) , catalog[ i , "output_filename" ] ) ; rm( this_file ) ; gc()
			
			if( !is.na( catalog[ i , 'output_folder' ] ) ){
		
				unzipped_files <- unzip_warn_fail( catalog[ i , "output_filename" ] , exdir = paste0( tempdir() , "/unzips" ) )

				for( this_zip in grep( "\\.zip" , unzipped_files , ignore.case = TRUE , value = TRUE ) ) unzipped_files <- c( unzipped_files , unzip_warn_fail( this_zip , exdir = paste0( tempdir() , "/unzips" ) ) )
								
				# stata or sascii
				if( grepl( "sta\\.zip$|stata\\.zip$" , catalog[ i , 'output_filename' ] , ignore.case = TRUE ) ){
				
					for( this_stata in grep( "\\.dta$" , unzipped_files , value = TRUE ) ){
					
						x <- data.frame( haven::read_dta( this_stata ) )
						
						# convert all column names to lowercase
						names( x ) <- tolower( names( x ) )

						catalog[ i , 'case_count' ] <- max( catalog[ i , 'case_count' ] , nrow( x ) , na.rm = TRUE )
						
						saveRDS( x , file = paste0( catalog[ i , 'output_folder' ] , "/" , tolower( gsub( "\\.dta" , ".rds" , basename( this_stata ) , ignore.case = TRUE ) ) ) , compress = FALSE )
						
						rm( x ) ; gc()
						
					}

				} else {
				
					dat_files <- grep( "\\.da$" , unzipped_files , value = TRUE , ignore.case = TRUE )
					
					sas_files <- grep( "\\.sas$" , unzipped_files , value = TRUE , ignore.case = TRUE )
										
					for( this_dat in dat_files ){

						this_sas <- sas_files[ tolower( gsub( "\\.sas" , "" , basename( sas_files ) , ignore.case = TRUE ) ) == tolower( gsub( "\\.da" , "" , basename( this_dat ) , ignore.case = TRUE ) ) ]
					
						x <- read_SAScii( this_dat , this_sas )		
	
						# note that the SAS script included a number of IF statements
						# that are not appropriately handled by the R SAScii package
						# in every case, these IF statements specify certain cases where
						# values should be overwritten with missing values instead.

						# load the SAS input script into memory as a big character string
						saslines <- readLines( this_sas )

						# keep only lines beginning with IF
						saslines <- saslines[ substr( saslines , 1 , 2 ) == "IF" ]

						# split them up by spaces
						sas.split <- strsplit( saslines , " " )

						# find the second element of the list, which contains the variable to overwrite
						overwrites <- sapply( sas.split , `[[` , 2 )

						# find the third element of the list, which contains equal or GE (>=)
						eoge <- sapply( sas.split , `[[` , 3 )

						# find the fourth element of the list, which contains the values to overwrite
						val <- sapply( sas.split , `[[` , 4 )

						if( identical( overwrites , list() ) ) overwrites <- NULL
						
						# loop through every 'overwrite' column instructed by the SAS script..
						for ( j in seq_along( overwrites ) ){

							try( {
								# if the line is 'greater than or equal to'..
								if ( eoge[ j ] == 'GE' ){

									# overwrite all records with values >= than the stated value with NA
									x[ no.na( x[ , overwrites[ j ] ] >= val[ j ] ) , overwrites[ j ] ] <- NA

								} else {

									# if the line is just 'equal to'..
									if ( eoge[ j ] == '=' ){

										# overwrite all records with values == to the stated value with NA
										x[ no.na( x[ , overwrites[ j ] ] == val[ j ] ) , overwrites[ j ] ] <- NA
										
									# otherwise..
									} else {
										# there's something else going on in the script that needs to be human-viewed
										stop( "eoge isn't GE or =" )
									}
								}
							} , silent = TRUE )
							
						}
						
						# convert all column names to lowercase
						names( x ) <- tolower( names( x ) )

						catalog[ i , 'case_count' ] <- max( catalog[ i , 'case_count' ] , nrow( x ) )

						saveRDS( x , file = paste0( catalog[ i , 'output_folder' ] , "/" , tolower( gsub( "\\.da" , ".rds" , basename( this_dat ) , ignore.case = TRUE ) ) ) , compress = FALSE )

						rm( x ) ; gc()
						
					}
					
				
				}
					
				# delete the temporary files
				suppressWarnings( file.remove( unzipped_files ) )

				cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'output_folder' ] , "'\r\n\n" ) )

			} else {
			
				cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )
			
			}
			
			suppressWarnings( file.remove( tf ) )
			
		}

		on.exit()
		
		catalog

	}

