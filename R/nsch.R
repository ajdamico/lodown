get_catalog_nsch <-
	function( data_name = "nsch" , output_dir , ... ){

	catalog <- NULL
	
	data_links <- readLines( "https://www.cdc.gov/nchs/slaits/nsch.htm" , warn = FALSE )
	
	dataset_lines <- grep( "ataset" , data_links , value = TRUE )
	
	dataset_hrefs <- paste0( gsub( '(.*)href=\"(.*)\"(.*)' , "\\2" , dataset_lines ) , "/" )

	four_digit_years <- as.numeric( gsub( "(.*)([0-9][0-9][0-9][0-9])(.*)" , "\\2" , dataset_hrefs ) )
	
	two_digit_years <- 2000 + as.numeric( gsub( "(.*)([0-9][0-9])(.*)" , "\\2" , dataset_hrefs ) )
	
	available_years <- 
		ifelse( !is.na( four_digit_years ) , four_digit_years , 
		ifelse( !is.na( two_digit_years ) , two_digit_years , 
			2003 ) )
			
	for( i in seq_along( available_years ) ){
	
		nsch_ftp_contents <- RCurl::getURL( dataset_hrefs[i] , ftp.use.epsv = TRUE, dirlistonly = TRUE )

		nsch_ftp_paths <- paste0( dataset_hrefs[i] , strsplit( nsch_ftp_contents , '\r\n' )[[1]] )

		dat_files <- nsch_ftp_paths[ !grepl( "mimp|Multiple Imputation" , basename( nsch_ftp_paths ) ) ]

		mi_files <- nsch_ftp_paths[ grepl( "mimp|Multiple Imputation" , basename( nsch_ftp_paths ) ) ]
		
		catalog <-
			rbind(
				catalog ,
				data.frame(
					directory = available_years[ i ] ,
					virgin_islands = grepl( "_vi" , basename( dat_files ) ) ,
					year = available_years[ i ] ,
					dat_url = dat_files ,
					mi_url = mi_files ,
					stringsAsFactors = FALSE
				)
			)
			
	}
	
	catalog$output_filename <- 
		paste0( 
			output_dir , "/" , 
			catalog$year , " " , ifelse( catalog$virgin_islands , "vi" , "main" ) ,	".rda" 
		)

	catalog

}


lodown_nsch <-
	function( catalog , data_name = "nsch" , ... ){

		if ( !requireNamespace( "haven" , quietly = TRUE ) ) stop( "haven needed for this function to work. to install it, type `install.packages( 'haven' )`" , call. = FALSE )

		tf <- tempfile()


		for ( i in seq_len( nrow( catalog ) ) ){

			# download the file
			cache_download( catalog[ i , "dat_url" ] , tf , mode = 'wb' )

			unzipped_files <- unzip( tf , exdir = dirname( catalog[ i , 'output_filename' ] ) )

			sas_path <- grep( "\\.sas7bdat$" , unzipped_files , value = TRUE )

			x <- data.frame( haven::read_sas( sas_path ) )

			file.remove( unzipped_files )
			
			# convert all column names to lowercase
			names( x ) <- tolower( names( x ) )

			# clear up RAM
			gc()

			# add a column of all ones
			x$one <- 1

			# download the multiply-imputed poverty data.frame
			cache_download( catalog[ i , "mi_url" ] , tf , mode = 'wb' )

			unzipped_files <- unzip( tf , exdir = dirname( catalog[ i , 'output_filename' ] ) )

			sas_path <- grep( "\\.sas7bdat$" , unzipped_files , value = TRUE )

			mimp <- data.frame( haven::read_sas( sas_path ) )

			file.remove( unzipped_files )
			
			# convert all column names to lowercase
			names( mimp ) <- tolower( names( mimp ) )

			# double-check that there's only the numbers 1 - 5
			# in the imputation column
			stopifnot( identical( as.numeric( 1:5 ) , sort( unique( mimp$imputation ) ) ) )

			# double-check that there are five times as many records
			# in the `mimp` data.frame as in `x`
			stopifnot( nrow( x ) == ( nrow( mimp ) / 5 ) )
					
			# loop through each unique level of the `imputation` field
			for ( impnum in 1:5 ){

				# keep the records for the current level,
				# and throw out that column simultaneously
				cur_imp <- 
					mimp[ 
						mimp$imputation %in% impnum ,
						!( names( mimp ) %in% 'imputation' ) 
					]
				
				# tack the imputed poverty values onto the main data.frame
				y <- merge( x , cur_imp )
				
				# triple-check that the number of records isn't changed
				stopifnot( nrow( x ) == nrow( y ) )
				
				# save the data.frame as `imp1` - `imp5`
				assign( paste0( 'imp' , impnum ) , y )
				
			}

			# save implicates 1 - 5 to the local working directory for faster loading later
			save( list = paste0( 'imp' , 1:5 ) , file = catalog[ i , 'output_filename' ] )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

		}

		# delete the temporary file
		file.remove( tf )

		invisible( TRUE )

	}

