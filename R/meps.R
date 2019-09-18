get_catalog_meps <-
	function( data_name = "meps" , output_dir , ... ){

		catalog <- NULL

		meps_dl_page <- "https://meps.ahrq.gov/mepsweb/data_stats/download_data_files.jsp"

		meps_dl_text <- RCurl::getURL( meps_dl_page , ssl.verifypeer = FALSE )

		year_start <- gregexpr( 'All available years' , meps_dl_text )[[1]][1]
		year_end <- gregexpr( 'Projected Years' , meps_dl_text )[[1]][1]

		possible_years <- substr( meps_dl_text , year_start , year_end )

		possible_years <- unique( as.numeric( strsplit( gsub( " +" , " " , gsub( "[^0-9]" , " " , possible_years ) ) , " " )[[1]] ) )

		possible_years <- possible_years[ !is.na( possible_years ) ]
		
		possible_years <- possible_years[ possible_years != 1996 ]

		for( this_year in possible_years ){

			cat( paste0( "loading " , data_name , " catalog for " , this_year , "\r\n\n" ) )

			search_page <- paste0( "https://meps.ahrq.gov/mepsweb/data_stats/download_data_files_results.jsp?cboDataYear=" , this_year , "&buttonYearandDataType=Search" )

			search_result <- strsplit( RCurl::getURL( search_page , ssl.verifypeer = FALSE ) , "(\r)?\n" )[[1]]
			
			puf_search_result_table <- grep( "PUF Search Results" , search_result )
			
			table_closures <- grep( "/table" , search_result )
			
			stopifnot( length( puf_search_result_table ) == 1 )
			
			search_result <- search_result[ seq( puf_search_result_table , min( table_closures[ table_closures > puf_search_result_table ] ) ) ]
			
			tf <- tempfile()

			writeLines( search_result , tf )

			available_pufs <- rvest::html_table( xml2::read_html( tf ) , fill = TRUE )[[1]]

			available_pufs <- available_pufs[ grepl( "^HC" , available_pufs[ , 1 ] ) , ]

			names( available_pufs ) <- c( "table_id" , "file_name" , "data_update" , "year" , "file_type" )
			
			for( i in seq_len( nrow( available_pufs ) ) ){
				
				this_line <- search_result[ grep( available_pufs[ i , 'table_id' ] , search_result ) ]

				this_line <- grep( "download_data_files_detail" , this_line , value = TRUE )
					
				if( grepl( "HC-036BRR" , this_line[ 1 ] ) ) this_line <- this_line[ 1 ]
					
				available_pufs[ i , 'this_link' ] <- unique( gsub( '(.*)href=\"(.*)\">(.*)</a>(.*)' , "\\2" , this_line ) )

				puf_result <- strsplit( RCurl::getURL( paste0( "https://meps.ahrq.gov/mepsweb/data_stats/" , available_pufs[ i , 'this_link' ] ) , ssl.verifypeer = FALSE ) , "(\r)?\n" )[[1]]
				
				link_names <- gsub( '(.*)href=\"(.*)\">(.*)</a>(.*)' , "\\2" , puf_result[ grepl( "ssp\\.zip" , puf_result ) ] )
				
				this_file <- merge( available_pufs[ i , ] , data.frame( full_url = paste0( "https://meps.ahrq.gov/" , gsub( "../" , "" , link_names , fixed = TRUE ) ) , file_num = if( length( link_names ) > 1 ) seq( link_names ) else NA , stringsAsFactors = FALSE ) )
			
				catalog <- rbind( catalog , this_file )
				
			}
			
		}

		catalog <- catalog[ !duplicated( catalog$full_url ) , ]
		
		catalog$output_filename <- 
			paste0( 
				output_dir , "/" , 
				ifelse( grepl( "-" , catalog$year ) , "" , paste0( catalog$year , "/" ) ) ,
				ifelse( grepl( "Longitudinal" , catalog$file_name ) , paste0( catalog$year , " " ) , "" ) ,
				
				ifelse( !grepl( "-" , catalog$year ) ,
					gsub( "[0-9][0-9][0-9][0-9] " , ""  , tolower( gsub( "[^A-z0-9 -]" , "" , catalog$file_name ) ) ) ,
					tolower( gsub( "[^A-z0-9 -]" , "" , catalog$file_name ) )
				) ,
				
				ifelse( is.na( catalog$file_num ) , "" , paste0( " f" , catalog$file_num ) ) ,
				".rds"
			)
			
		catalog <- catalog[ grepl( "\\.zip$" , catalog$full_url , ignore.case = TRUE ) , ]
			
		catalog$output_filename <- gsub( " data| public use| file" , "" , catalog$output_filename )
		
		catalog
		
	}

	

lodown_meps <-
	function( data_name = "meps" , catalog , ... ){

		on.exit( print( catalog ) )

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){
			
			# download the file
			this_file <- cachaca( catalog[ i , 'full_url' ] , FUN = httr::GET , config = httr::config( ssl_verifypeer = FALSE ) , filesize_fun = 'unzip_verify' )
			
			writeBin( httr::content( this_file , "raw" ) , tf )

			unzipped_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , "/unzips" ) )

			if( length( unzipped_files ) != 1 ) stop( "expecting a single sas transport file" )

			import_result <-
				try({
					x <- foreign::read.xport( unzipped_files )

					# convert all column names to lowercase
					names( x ) <- tolower( names( x ) )

					catalog[ i , 'case_count' ] <- nrow( x )
					
					saveRDS( x , file = catalog[ i , 'output_filename' ] , compress = FALSE )
				} , silent = TRUE )
				
			if( class( import_result ) == 'try-error' ) cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " failed.'\r\n\n" ) )
				
			# delete the temporary files
			suppressWarnings( file.remove( tf , unzipped_files ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

		}

		on.exit()
		
		catalog

	}

