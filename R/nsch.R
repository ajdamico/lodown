get_catalog_nsch <-
	function( data_name = "nsch" , output_dir , ... ){

	catalog <- NULL
	
	data_links <- readLines( "https://www.cdc.gov/nchs/slaits/nsch.htm" , warn = FALSE )
	
	dataset_lines <- grep( "ataset" , data_links , value = TRUE )
	
	dataset_hrefs <- gsub( "^ftp" , "https" , paste0( gsub( '(.*)href=\"(.*)\"(.*)' , "\\2" , dataset_lines ) , "/" ) )

	four_digit_years <- suppressWarnings( as.numeric( gsub( "(.*)([0-9][0-9][0-9][0-9])(.*)" , "\\2" , dataset_hrefs ) ) )
	
	two_digit_years <- 2000 + suppressWarnings( as.numeric( gsub( "(.*)([0-9][0-9])(.*)" , "\\2" , dataset_hrefs ) ) )
	
	available_years <- 
		ifelse( !is.na( four_digit_years ) , four_digit_years , 
		ifelse( !is.na( two_digit_years ) , two_digit_years , 
			2003 ) )
			
	for( i in seq_along( available_years ) ){
	
		nsch_ftp_contents <- strsplit( RCurl::getURL( dataset_hrefs[i] , ssl.verifypeer = FALSE ) , "<br>" )[[1]]
		nsch_ftp_contents <- gsub( '(.*)\\">(.*)<\\/A>$', "\\2" , nsch_ftp_contents )
		nsch_ftp_paths <- nsch_ftp_contents[ grep( "\\.zip$" , tolower( nsch_ftp_contents ) ) ]
		
		nsch_ftp_paths <- gsub( " " , "%20" , nsch_ftp_paths , fixed = TRUE )
		
		dat_files <- nsch_ftp_paths[ !grepl( "mimp|Multiple%20Imputation" , basename( nsch_ftp_paths ) ) ]

		mi_files <- nsch_ftp_paths[ grepl( "mimp|Multiple%20Imputation" , basename( nsch_ftp_paths ) ) ]
		
		catalog <-
			rbind(
				catalog ,
				data.frame(
					directory = available_years[ i ] ,
					virgin_islands = grepl( "_vi" , basename( dat_files ) ) ,
					year = available_years[ i ] ,
					screener_url = NA ,
					dat_url = paste0( dataset_hrefs[i] , dat_files ) ,
					mi_url = paste0( dataset_hrefs[i] , mi_files ) ,
					stringsAsFactors = FALSE
				)
			)
			
	}
	
	
	
	data_links <- readLines( "https://www.census.gov/programs-surveys/nsch/data/datasets.html" , warn = FALSE )
	
	dataset_lines <- grep( "https(.*)datasets(.*)nsch" , data_links , value = TRUE , ignore.case = TRUE )
	
	dataset_hrefs <- unique( paste0( "https://www.census.gov/" , gsub( '(.*)href=\"(.*)html(.*)' , "\\2html" , dataset_lines ) ) )
	
	dataset_hrefs <- dataset_hrefs[ !grepl( 'linkedin' , dataset_hrefs ) ]
	
	four_digit_years <- suppressWarnings( as.numeric( gsub( "(.*)([0-9][0-9][0-9][0-9])(.*)" , "\\2" , dataset_hrefs ) ) )
	
	

	catalog <-
		rbind(
			catalog ,
			data.frame(
				directory = four_digit_years ,
				virgin_islands = FALSE ,
				year = four_digit_years ,
				dat_url = paste0( "https://www2.census.gov/programs-surveys/nsch/datasets/" , four_digit_years , "/nsch_" , four_digit_years , "_topical" , ifelse( four_digit_years >= 2018 , "_SAS" , "" ) , ".zip" ) ,
				screener_url = paste0( "https://www2.census.gov/programs-surveys/nsch/datasets/" , four_digit_years , "/nsch_" , four_digit_years , "_screener" , ifelse( four_digit_years >= 2018 , "_SAS" , "" ) , ".zip" ) ,
				mi_url = ifelse( four_digit_years == 2016 , "https://www2.census.gov/programs-surveys/nsch/datasets/2016/nsch_2016_implicate.zip" , NA ) ,
				stringsAsFactors = FALSE
			)
		)
	

	
	catalog$output_folder <- output_dir
	
	catalog[ order( catalog$year ) , ]

}


lodown_nsch <-
	function( data_name = "nsch" , catalog , ... ){

		on.exit( print( catalog ) )

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			# download the file
			cachaca( catalog[ i , "dat_url" ] , tf , mode = 'wb' , filesize_fun = 'unzip_verify' )

			unzipped_files <- unzip_warn_fail( tf , exdir = tempdir() )

			sas_path <- grep( "\\.sas7bdat$" , unzipped_files , value = TRUE )

			x <- data.frame( haven::read_sas( sas_path ) )

			file.remove( unzipped_files )
			
			# convert all column names to lowercase
			names( x ) <- tolower( names( x ) )

			# clear up RAM
			gc()

			# add a column of all ones
			x$one <- 1

			if( catalog[ i , 'year' ] >= 2017 ){
			
				mimp <- x[ , c( 'hhid' , paste0( 'fpl_i' , 1:6 ) ) ]
				x <- x[ , !( names( x ) %in% paste0( 'fpl_i' , 1:6 ) ) ]
			
			} else {

				# download the multiply-imputed poverty data.frame
				cachaca( catalog[ i , "mi_url" ] , tf , mode = 'wb' , filesize_fun = 'unzip_verify' )

				unzipped_files <- unzip_warn_fail( tf , exdir = tempdir() )

				sas_path <- grep( "\\.sas7bdat$" , unzipped_files , value = TRUE )

				mimp <- data.frame( haven::read_sas( sas_path ) )

				file.remove( unzipped_files )
				
				# convert all column names to lowercase
				names( mimp ) <- tolower( names( mimp ) )

			}


			num_imps <- if( catalog[ i , 'year' ] >= 2016 ) 6 else 5
			
			if( catalog[ i , 'year' ] >= 2016 ){

				mout <- NULL
				for( j in seq( num_imps ) ) {
				
					tout <- mimp[ c( 'hhid' , paste0( "fpl_i" , j ) ) ]
					names( tout ) <- c( "hhid" , "fpl" )
					tout$imputation <- j
					mout <- rbind( mout , tout ) ; rm( tout ) ; gc()
				}
				
				mimp <- mout ; rm( mout ) ; gc()
			
				x$fpl <- NULL
			
			}
				
			# double-check that there are five times as many records
			# in the `mimp` data.frame as in `x`
			stopifnot( nrow( x ) == ( nrow( mimp ) / num_imps ) )
		
			# loop through each unique level of the `imputation` field
			for ( impnum in seq( num_imps ) ){

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

			catalog[ i , 'case_count' ] <- nrow( y )
			
			rm( y ) ; gc()
				
			# save implicates 1 - 5 to the local working directory for faster loading later
			saveRDS( 
				mget( paste0( "imp" , seq( num_imps ) ) ) , 
				file = paste0( catalog[ i , 'output_folder' ] , "/" , catalog[ i , 'year' ] , ' ' , if( catalog[ i , 'virgin_islands' ] ) 'vi' else 'main' , '.rds' ) , 
				compress = FALSE 
			) ; rm( list = paste0( 'imp' , seq( num_imps ) ) ) ; rm( x ) ; gc()

			if( !is.na( catalog[ i , 'screener_url' ] ) ){
				
				# download the file
				cachaca( catalog[ i , "dat_url" ] , tf , mode = 'wb' )

				unzipped_files <- unzip_warn_fail( tf , exdir = tempdir() )

				sas_path <- grep( "\\.sas7bdat$" , unzipped_files , value = TRUE )

				x <- data.frame( haven::read_sas( sas_path ) )

				file.remove( unzipped_files )
				
				# convert all column names to lowercase
				names( x ) <- tolower( names( x ) )

				# clear up RAM
				gc()

				# add a column of all ones
				x$one <- 1

				saveRDS( x , file = paste0( catalog[ i , 'output_folder' ] , '/' , catalog[ i , 'year' ] , ' screener.rds' ) , compress = FALSE )
			
			} ; rm( x ) ; gc()
			
			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'output_folder' ] , "'\r\n\n" ) )

		}

		# delete the temporary file
		file.remove( tf )

		on.exit()
		
		catalog

	}

