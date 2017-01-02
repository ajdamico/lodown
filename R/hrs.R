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

			all_links <- rvest::html_nodes( httr::content( this_resp ) , "a" )
			
			link_text <- rvest::html_text( all_links )
			
			link_refs <- rvest::html_attr( all_links , "href" )
			
			stopifnot( length( link_text ) == length( link_refs ) )
			
			these_versids <- paste0( "https://ssl.isr.umich.edu/hrs/" , link_refs[ grepl( 'filedownload2\\.php\\?d' , link_refs ) ] )
			
			this_text <- stringr::str_trim( link_text[ grepl( 'filedownload2\\.php\\?d' , link_refs ) ] )
			
			catalog <-
				rbind(
					catalog ,
					data.frame(
						file_title = versid_text[ this_page ] ,
						file_name = this_text ,
						full_url = these_versids ,
						stringsAsFactors = FALSE
					)
				)
				
		}
		
		catalog$year <- ifelse( grepl( "^[0-9][0-9][0-9][0-9]" , catalog$file_title ) , substr( catalog$file_title , 1 , 4 ) , NA )
		
		catalog$output_filename <- paste0( output_dir , "/" , ifelse( is.na( catalog$year ) , "" , paste0( catalog$year , "/" ) ) , catalog$file_name )
		
		catalog
		
	}
						
		
lodown_hrs <-
	function( data_name = "hrs" , catalog , ... ){

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
			this_file <- cachaca( catalog[ i , "full_url" ] , FUN = httr::GET , filesize_fun = 'httr' )

			writeBin( httr::content( this_file , "raw" ) , catalog[ i , "output_filename" ] )
			
			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

		}

		invisible( TRUE )

	}

