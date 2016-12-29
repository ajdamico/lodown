get_catalog_anes <-
	function( data_name = "anes" , output_dir , ... ){

		if ( !requireNamespace( "purrr" , quietly = TRUE ) ) stop( "purrr needed for this function to work. to install it, type `install.packages( 'purrr' )`" , call. = FALSE )

		if( !( 'your_email' %in% names(list(...)) ) ) stop( "`your_email` parameter must be specified.  create an account at http://www.electionstudies.org/studypages/download/registration_form.php" )
		
		your_email <- list(...)[["your_email"]]
						
		# contact the anes website to log in
		httr::POST( "http://www.electionstudies.org/studypages/download/login-process.php" , body = list( "email" = your_email ) )

		# download the `all_datasets` page to figure out what needs to be downloaded
		z <- httr::content( httr::GET( "http://www.electionstudies.org/studypages/download/datacenter_all_datasets.php" ) )

		# http://stackoverflow.com/a/41380643/1759499
		nested_links <- purrr::map( rvest::html_nodes(z, "article") , ~ rvest::html_attr( rvest::html_nodes( . , "a" ) , "href" ) )
		
		study_names <- stringr::str_trim( sapply( strsplit( rvest::html_text( rvest::html_nodes( z , "article" ) ) , "\\r\\n" ) , "[[" , 2 ) )
		
		dta_files <- lapply( nested_links , function( x ) grep( "dta(.*)zip" , x , value = TRUE , ignore.case = TRUE ) )
		
		sav_files <- lapply( nested_links , function( x ) grep( "sav(.*)zip" , x , value = TRUE , ignore.case = TRUE ) )
		
		stopifnot( length( study_names ) == length( dta_files ) )
		stopifnot( length( study_names ) == length( sav_files ) )
		
		study_names <- gsub( "ANES" , "" , gsub( "-" , "_" , study_names ) )
		
		study_names <- stringr::str_trim( gsub( "[^0-9A-z ]" , "" , study_names ) )
		
		available_dtas <- lapply( dta_files , function( x ) length( x ) > 0 )

		files_to_download <- ifelse( available_dtas , dta_files , sav_files )
		
		catalog <- do.call( rbind , mapply( merge , files_to_download , study_names ) )
		
		names( catalog ) <- c( 'full_url' , 'directory' )
		
		catalog[ , ] <- sapply( catalog[ , ] , as.character )
		
		catalog$full_url <- gsub( "../" , "http://www.electionstudies.org/studypages/" , catalog$full_url , fixed = TRUE )

		catalog$output_filename <- paste0( output_dir , "/" , catalog$directory , "/" , gsub( "(dta|sav)(.*)zip" , "" , basename( catalog$full_url ) ) , ".rda" )
		
		catalog

	}


lodown_anes <-
	function( data_name = "anes" , catalog , ... ){

		tf <- tempfile()

		if( !( 'your_email' %in% names(list(...)) ) ) stop( "`your_email` parameter must be specified.  create an account at http://www.electionstudies.org/studypages/download/registration_form.php" )
		
		your_email <- list(...)[["your_email"]]

		# contact the anes website to log in
		httr::POST( "http://www.electionstudies.org/studypages/download/login-process.php" , body = list( "email" = your_email ) )
		
		for ( i in seq_len( nrow( catalog ) ) ){

			# download the file
			this_file <- cachaca( catalog[ i , "full_url" ] , FUN = httr::GET , filesize_fun = 'httr' )

			writeBin( this_file$content , tf ) ; rm( this_file )
			
			unzipped_files <- unzip( tf , exdir = paste0( tempdir() , "/unzips" ) )

			for( stata12 in grep( "stata12" , unzipped_files , value = TRUE ) ){
				
				file.remove( stata12 )
				
				unzipped_files <- unzipped_files[ unzipped_files != stata12 ]
				
			}
			
			if( grepl( "\\.dta$" , unzipped_files , ignore.case = TRUE ) ){
			
				path_to_dta <- grep( "\\.dta$" , unzipped_files , ignore.case = TRUE , value = TRUE )
				
				if( length( path_to_dta ) != 1 ) stop( "not prepared for multiple files within one zipped file" )
				
				x <- data.frame( haven::read_dta( path_to_dta ) )
				
			} else {
			
				path_to_sav <- grep( "\\.sav$" , unzipped_files , ignore.case = TRUE , value = TRUE )
				
				if( length( path_to_sav ) != 1 ) stop( "not prepared for multiple files within one zipped file" )
				
				x <- data.frame( haven::read_spss( path_to_sav ) )
				
			}

			# convert all column names to lowercase
			names( x ) <- tolower( names( x ) )

			save( x , file = catalog[ i , 'output_filename' ] )

			# delete the temporary files
			file.remove( tf , unzipped_files )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

		}

		invisible( TRUE )

	}

