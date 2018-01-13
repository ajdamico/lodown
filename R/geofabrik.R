get_catalog_geofabrik <-
	function( data_name = "geofabrik" , output_dir , ... ){

		this_page <- geofabrik_extract_links( "http://download.geofabrik.de/index.html" )

		this_level <- data.frame( this_page , level_one = NA , level_two = NA , level_three = NA , level_four = NA )

		for( i in seq( nrow( this_level ) ) ){

			if( grepl( "\\[" , this_level[ i , 'link_text' ] ) ) this_level[ i , 'level_one' ] <- this_level[ i - 1 , 'level_one' ] else this_level[ i , 'level_one' ] <- this_level[ i , 'link_text' ]
			
		}

		level_one_links_to_follow <- this_level[ grepl( "html$" , this_level$full_url ) , ]

		level_one_links_to_store <- this_level[ !grepl( "html$" , this_level$full_url ) , ]


		level_two_links_to_follow <- level_two_links_to_store <- NULL

		for( j in seq( nrow( level_one_links_to_follow ) ) ){

			this_level <- geofabrik_extract_links( level_one_links_to_follow[ j , 'full_url' ] )

			for( i in seq( nrow( this_level ) ) ){

				if( grepl( "\\[" , this_level[ i , 'link_text' ] ) ) this_level[ i , 'level_two' ] <- this_level[ i - 1 , 'level_two' ] else this_level[ i , 'level_two' ] <- this_level[ i , 'link_text' ]
			
				this_level[ i , 'level_one' ] <- level_one_links_to_follow[ j , 'level_one' ]
				
				this_level$level_three <- this_level$level_four <- NA
			
			}

			level_two_links_to_follow <- rbind( level_two_links_to_follow , this_level[ grepl( "html$" , this_level$full_url ) , ] )

			level_two_links_to_store <- rbind( level_two_links_to_store , this_level[ !grepl( "html$" , this_level$full_url ) , ] )
			
		}
			

		level_three_links_to_follow <- level_three_links_to_store <- NULL

		for( j in seq( nrow( level_two_links_to_follow ) ) ){

			this_level <- geofabrik_extract_links( level_two_links_to_follow[ j , 'full_url' ] )

			for( i in seq( nrow( this_level ) ) ){

				if( grepl( "\\[" , this_level[ i , 'link_text' ] ) ) this_level[ i , 'level_three' ] <- this_level[ i - 1 , 'level_three' ] else this_level[ i , 'level_three' ] <- this_level[ i , 'link_text' ]
			
				this_level[ i , 'level_one' ] <- level_two_links_to_follow[ j , 'level_one' ]
				this_level[ i , 'level_two' ] <- level_two_links_to_follow[ j , 'level_two' ]
				
				this_level$level_four <- NA
			
			}

			level_three_links_to_follow <- rbind( level_three_links_to_follow , this_level[ grepl( "html$" , this_level$full_url ) , ] )

			level_three_links_to_store <- rbind( level_three_links_to_store , this_level[ !grepl( "html$" , this_level$full_url ) , ] )
			
		}
			

			
		level_four_links_to_follow <- level_four_links_to_store <- NULL

		for( j in seq( nrow( level_three_links_to_follow ) ) ){

			this_level <- geofabrik_extract_links( level_three_links_to_follow[ j , 'full_url' ] )

			for( i in seq( nrow( this_level ) ) ){

				if( grepl( "\\[" , this_level[ i , 'link_text' ] ) ) this_level[ i , 'level_four' ] <- this_level[ i - 1 , 'level_four' ] else this_level[ i , 'level_four' ] <- this_level[ i , 'link_text' ]
			
				this_level[ i , 'level_one' ] <- level_three_links_to_follow[ j , 'level_one' ]
				this_level[ i , 'level_two' ] <- level_three_links_to_follow[ j , 'level_two' ]
				this_level[ i , 'level_three' ] <- level_three_links_to_follow[ j , 'level_three' ]
				
			}

			# level_four_links_to_follow <- rbind( level_four_links_to_follow , this_level[ grepl( "html$" , this_level$full_url ) , ] )

			level_four_links_to_store <- rbind( level_four_links_to_store , this_level[ !grepl( "html$" , this_level$full_url ) , ] )
			
		}
			

		catalog <- rbind( level_one_links_to_store , level_two_links_to_store , level_three_links_to_store , level_four_links_to_store )

		names( catalog )[ names( catalog ) == 'full_url' ] <- 'full_url'
		
		catalog$output_filename <- paste0( output_dir , "/" , catalog$level_one , "/" )
		catalog$output_filename <- ifelse( is.na( catalog$level_two ) , catalog$output_filename , paste0( catalog$output_filename , "/" , catalog$level_two ) )
		catalog$output_filename <- ifelse( is.na( catalog$level_three ) , catalog$output_filename , paste0( catalog$output_filename , "/" , catalog$level_three ) )
		catalog$output_filename <- ifelse( is.na( catalog$level_four ) , catalog$output_filename , paste0( catalog$output_filename , "/" , catalog$level_four ) )
		catalog$output_filename <- paste0( catalog$output_filename , "/" , basename( catalog$full_url ) )
		
		catalog$output_filename <- ifelse( !grepl( "\\.zip$" , catalog$output_filename ) , catalog$output_filename , paste0( gsub( "\\.zip$" , "" , catalog$output_filename ) , "/" , basename( catalog$output_filename ) ) )
		
		# remove files
		catalog <- catalog[ tools::file_ext( catalog$level_two ) %in% c( "" , NA ) , ]
		catalog <- catalog[ tools::file_ext( catalog$level_three ) %in% c( "" , NA ) , ]
		catalog <- catalog[ tools::file_ext( catalog$level_four ) %in% c( "" , NA ) , ]
		
		# remove non-files
		catalog <- catalog[ !( tools::file_ext( catalog$full_url ) %in% c( "" , NA ) ) , ]
		
		catalog

	}


lodown_geofabrik <-
	function( data_name = "geofabrik" , catalog , ... ){

		on.exit( print( catalog ) )

		for ( i in seq_len( nrow( catalog ) ) ){

			# download the file
			cachaca( catalog[ i , "full_url" ] , catalog[ i , 'output_filename' ] , mode = 'wb' )

			if( grepl( "bz2$" , catalog[ i , 'output_filename' ] ) ) {
			
				this_result <- R.utils::bunzip2( catalog[ i , 'output_filename' ] )
			
				cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , this_result , "'\r\n\n" ) )
				
			} else if( grepl( "zip$" , catalog[ i , 'output_filename' ] ) ) {
			
				unzipped_files <- unzip_warn_fail( catalog[ i , 'output_filename' ] , exdir = np_dirname( catalog[ i , 'output_filename' ] ) )
			
				cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , np_dirname( catalog[ i , 'output_filename' ] ) , "'\r\n\n" ) )
				
				suppressWarnings( file.remove( catalog[ i , 'output_filename' ] ) )
				
			} else {
			
				cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

			}
			
			catalog[ i , 'case_count' ] <- NA

		}

		on.exit()
		
		catalog

	}


	
	
geofabrik_extract_links <-
	function( this_url ){

		all_links <- rvest::html_nodes( xml2::read_html( this_url ) , "a" )

		link_text <- rvest::html_text( all_links )

		link_refs <- rvest::html_attr( all_links , "href" )

		link_list <- data.frame( link_text = link_text , full_url = link_refs , stringsAsFactors = FALSE )
		
		link_list <- link_list[ !grepl( "http" , link_list$full_url ) , ]
		
		link_list$full_url <- paste0( gsub( "(.*)/(.*)" , "\\1" , this_url ) , "/" , gsub( "^\\." , "" , link_list$full_url ) )
		
		if( "raw directory index" %in% link_list$link_text ) link_list <- link_list[ seq( which( "raw directory index" == link_list$link_text ) + 1 , nrow( link_list ) ) , ]
		
		if( "Technical details" %in% link_list$link_text ) link_list <- link_list[ seq( 1 , which( "Technical details" == link_list$link_text ) - 1 ) , ]
		
		link_list
	}
	
