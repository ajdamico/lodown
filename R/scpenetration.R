get_catalog_scpenetration <-
	function( data_name = "scpenetration" , output_dir , ... ){

		catalog <- NULL
	
		for( ma_pd in c( "MA" , "PDP" ) ){
		
			pene_url <- paste0( "https://www.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/MCRAdvPartDEnrolData/" , ma_pd , "-State-County-Penetration.html" )

			all_dates <- rvest::html_table( xml2::read_html( pene_url ) )

			all_dates <- all_dates[[1]][ , "Report Period" ]

			all_links <- rvest::html_nodes( xml2::read_html( pene_url ) , xpath = '//td/a' )

			prefix <- "https://www.cms.gov/"

			all_links <- gsub( '<a href=\"' , prefix , all_links )
			all_links <- gsub( "\">(.*)" , "" , all_links )

			this_catalog <-
			  data.frame(
				  output_filename = paste0( output_dir , "/" , tolower( ma_pd ) , "_sc penetration.rds" ) ,
				  full_url = as.character( all_links ) ,
				  year_month = all_dates ,
				  stringsAsFactors = FALSE
			  )

			for( this_row in seq( nrow( this_catalog ) ) ){
				
				link_text <- readLines( this_catalog[ this_row , 'full_url' ] )
				link_line <- grep( "zip" , link_text , value = TRUE )
				link_line <- gsub( '(.*) href=\"' , "" , gsub( '(.*) href=\"/' , prefix , link_line ) )
				this_catalog[ this_row , 'full_url' ] <- gsub( '\">(.*)' , "" , link_line )

			}
	
			this_catalog$ma_pd <- ma_pd
			
			catalog <- rbind( catalog , this_catalog )
			
		}
		
		catalog[ order( catalog$year_month ) , ]
		
	}


lodown_scpenetration <-
	function( data_name = "scpenetration" , catalog , ... ){

		on.exit( print( catalog ) )

		tf <- tempfile()

		unique_savefiles <- unique( catalog$output_filename )
		
		for( this_savefile in unique_savefiles ){

			these_entries <- catalog[ catalog$output_filename == this_savefile , ]

			this_result <- NULL
			
			for ( i in seq_len( nrow( these_entries ) ) ){

				# download the file
				cachaca( these_entries[ i , "full_url" ] , tf , mode = 'wb' )


				# extract the contents of the zipped file
				# into the current year-month-specific directory
				# and (at the same time) create an object called
				# `unzipped_files` that contains the paths on
				# your local computer to each of the unzipped files
				unzipped_files <- unzip_warn_fail( tf , exdir = np_dirname( these_entries[ i , 'output_filename' ] ) )

				x <- data.frame( readr::read_csv( grep( "State_County(.*)\\.csv$" , unzipped_files , value = TRUE , ignore.case = TRUE ) , guess_max = 100000 ) )

				x$year_month <- these_entries[ i , 'year_month' ]
				
				x <- unique( x )

				names( x ) <- tolower( names( x ) )
				
				names( x ) <- gsub( "\\." , "_" , names( x ) )

				x$eligibles <- as.numeric( gsub( "," , "" , x$eligibles ) )

				x$enrolled <- as.numeric( gsub( "," , "" , x$enrolled ) )

				x$penetration <- as.numeric( gsub( "\\%" , "" , x$penetration ) )
				
				this_result <- rbind( this_result , x )

				# add the number of records to the catalog
				catalog[ catalog$output_filename == this_savefile , ][ i , 'case_count' ] <- nrow( x )
				
				# delete the temporary files
				file.remove( tf , unzipped_files )

			}
			
			saveRDS( this_result , file = this_savefile , compress = FALSE )

			cat( paste0( data_name , " catalog entry " , which( this_savefile == unique_savefiles ) , " of " , length( unique_savefiles ) , " stored at '" , this_savefile , "'\r\n\n" ) )

		}

		on.exit()
		
		catalog

	}

