get_catalog_acs <-
	function( data_name = "acs" , output_dir , include_puerto_rico = TRUE , ... ){

		if ( !requireNamespace( "archive" , quietly = TRUE ) ) stop( "archive needed for this function to work. to install it, type `devtools::install_github( 'jimhester/archive' )`" , call. = FALSE )

		catalog <- NULL
		
		h_basenames <- paste0( "csv_h" , tolower( c( state.abb , "DC" , "PR" ) ) , ".zip" )
		
		p_basenames <- paste0( "csv_p" , tolower( c( state.abb , "DC" , "PR" ) ) , ".zip" )
		
		pums_ftp <- "https://www2.census.gov/programs-surveys/acs/data/pums/"
	
		ftp_listing <- rvest::html_table( xml2::read_html( pums_ftp ) )[[1]][ , "Name" ]

		suppressWarnings( available_years <- as.numeric( gsub( "/" , "" , ftp_listing ) ) )
		
		available_years <- available_years[ !is.na( available_years ) ]
		
		# remove files prior to 2005
		available_years <- available_years[ available_years >= 2005 ]
		
		for( this_year in available_years ){
		
			cat( paste0( "loading " , data_name , " catalog from " , paste0( pums_ftp , this_year ) , "\r\n\n" ) )

			if( this_year < 2007 ){
			
				available_periods <- "1-Year"
				
				available_folders <- paste0( pums_ftp , this_year )
				
			} else {
			
				ftp_listing <- rvest::html_table( xml2::read_html( paste0( pums_ftp , this_year ) ) )[[1]][ , "Name" ]
			
				available_periods <- gsub( "/$" , "" , grep( "-Year" , ftp_listing , value = TRUE ) )
			
				available_folders <- paste0( pums_ftp , this_year , "/" , available_periods )
			
			}
			
			for( i in seq_along( available_folders ) ){
			
				this_tablename <- paste0( "acs" , this_year , "_" , substr( available_periods[ i ] , 1 , 1 ) , "yr" )
			
				catalog <-
					rbind( 
						catalog ,
						data.frame(
							year = this_year ,
							time_period = available_periods[ i ] ,
							stateab = tolower( c( state.abb , "DC" , "PR" ) ) ,
							h_full_url = paste0( available_folders[ i ] , "/" , h_basenames ) ,
							p_full_url = paste0( available_folders[ i ] , "/" , p_basenames ) ,
							merged_tablename = paste0( output_dir , "/acs" , this_year , "_" , substr( available_periods[ i ] , 1 , 1 ) , "yr.rds" ) ,
							output_folder = paste0( output_dir , "/" , this_year , "/" , available_periods[ i ] , "/" ) ,
							stringsAsFactors = FALSE
						)
					)
					
			}
			
		}
		
		catalog
	
	}

lodown_acs <-
	function( data_name = "acs" , catalog , ... ){

		on.exit( print( catalog ) )

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			for( j in c( "h" , "p" ) ){

				# download the wyoming structure file
				wyoming_unix <- paste0( dirname( catalog[ i , 'h_full_url' ] ) , "/unix_" , j , "wy.zip" )
				
				cachaca( wyoming_unix , tf , mode = 'wb' )

				unzipped_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , "/unzips" ) )

				wyoming_table <- data.frame( haven::read_sas( unzipped_files[ grep( 'sas7bdat' , unzipped_files ) ] ) )
				
				names( wyoming_table ) <- tolower( names( wyoming_table ) )
				
				cc <- ifelse( unlist( sapply( wyoming_table , class ) ) == 'numeric' , 'n' , 'c' )

				cachaca( catalog[ i , if( j == 'h' ) 'h_full_url' else 'p_full_url' ] , tf , mode = 'wb' )
				
				archive::archive_extract( tf , dir = paste0( tempdir() , "/unzips" ) )

				tfn <- list.files( paste0( tempdir() , "/unzips" ) , full.names = TRUE )

				# limit the files to read in to ones containing csvs
				fn <- grep( '\\.csv$' , tfn , value = TRUE )

				stopifnot( length( fn ) == 1 )
				
				if( j == 'h' ) h_names <- tolower( names( wyoming_table ) )
				
				x <-
					data.frame( 
						readr::read_csv( 
							fn , 
							col_names = tolower( names( wyoming_table ) ) , 
							col_types = paste0( cc , collapse = "" ) ,
							skip = 1
						) 
					)
					
				# remove overlapping field names, except rt and serialno
				if( j == 'p' ) x <- x[ c( 'rt' , 'serialno' , names( x )[ !( names( x ) %in% h_names ) ] ) ]
					
				x$one <- 1
				
				
					
				# special exception for the 2009 3-year file..  too many missings in the weights.
				if( catalog[ i , 'year' ] <= 2009 & catalog[ i , 'time_period' ] %in% c( '3-Year' , '5-Year' ) ){
				
					# identify all weight columns
					wgt_cols <- grep( "wgt" , names( x ) , value = TRUE )
					
					# replace missings with zeroes
					x[ wgt_cols ][ is.na( x[ wgt_cols ] ) ] <- 0
					
				}
					
				saveRDS( x , file = paste0( catalog[ i , 'output_folder' ] , "/" , j , catalog[ i , 'stateab' ] , '.rds' ) , compress = FALSE ) ; 
				
				# add the number of records to the catalog
				if( j == 'p' ){
					p_table <- x
					catalog[ i , 'case_count' ] <- nrow( x )
				} else h_table <- x
				
				rm( x ) ; gc()
				
				# these files require lots of temporary disk space,
				# so delete them once they're part of the database
				suppressWarnings( file.remove( tfn ) )
				
			}

			h_table$rt <- p_table$rt <- NULL
			
			x <- merge( h_table , p_table ) ; rm( h_table , p_table ) ; gc()

			x$rt <- "M"
			
			
			saveRDS( x , file = paste0( catalog[ i , 'output_folder' ] , "/m" , catalog[ i , 'stateab' ] , '.rds' ) , compress = FALSE ) ; 
			
			stopifnot( nrow( x ) == catalog[ i , 'case_count' ] )
			
			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'output_folder' ] , "'\r\n\n" ) )
			
		}
		
		
		# loop through all merged table names
		merged_tables <- unique( catalog[ , c( 'year' , 'time_period' , 'output_folder' , 'merged_tablename' ) ] )
		
		for( i in seq_len( nrow( merged_tables ) ) ){
		
			records_to_stack_and_merge <-
				catalog[ catalog$merged_tablename == merged_tables[ i , 'merged_tablename' ] , ]
				
			m_stacks <- paste0( records_to_stack_and_merge$output_folder , "/m" , records_to_stack_and_merge$stateab , '.rds' )
			
			x <- NULL
			for( this_m in m_stacks ) x <- rbind( x , readRDS( this_m ) )

			saveRDS( x , file = merged_tables[ i , 'merged_tablename' ] , compress = FALSE ) ; rm( x ) ; gc()
		
		}

		
		on.exit()
				
		catalog

	}
