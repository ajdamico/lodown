get_catalog_ssa <-
	function( data_name = "ssa" , output_dir , ... ){

		catalog <- NULL
		
		data_page <- "https://www.ssa.gov/policy/docs/data/index.html"
	
		link_page <- rvest::html_nodes( xml2::read_html( data_page ) , "a" )
		
		link_text <- rvest::html_text( link_page )
		
		link_refs <- rvest::html_attr( link_page , "href" )
		
		microdata_text <- link_text[ grep( "/microdata/" , link_refs ) ]
		
		microdata_refs <- xml2::url_absolute( link_refs[ grep( "/microdata/" , link_refs ) ] , data_page )
		
		# skip the new beneficiary data system
		for( this_num in which( !grepl( "/nbds/" , microdata_refs ) ) ){
		
			this_page <- microdata_refs[ this_num ]
		
			the_links <- rvest::html_nodes( xml2::read_html( this_page ) , "a" )
			
			the_text <- rvest::html_text( the_links )
			
			the_refs <- rvest::html_attr( the_links , "href" )
	
			zip_text <- the_text[ grep( "\\.zip$" , the_refs , ignore.case = TRUE ) ]
			
			zip_refs <- the_refs[ grep( "\\.zip$" , the_refs , ignore.case = TRUE ) ]

			this_pdf <- grep( "dictionary\\.pdf" , the_refs , ignore.case = TRUE , value = TRUE )
			
			stopifnot( length( this_pdf ) == 1 )
			
			this_cat <-
				data.frame(
					full_url = xml2::url_absolute( zip_refs , microdata_refs[ this_num ] )  ,
					description = zip_text ,
					doc_url = xml2::url_absolute( this_pdf , microdata_refs[ this_num ] )  ,
					stringsAsFactors = FALSE
				)
				
			catalog <- rbind( catalog , this_cat )
			
		}
		
		# remove duplicates
		duplicates <- c( "https://www.ssa.gov/policy/docs/microdata/earn/benefits04text.zip" , "https://www.ssa.gov/policy/docs/microdata/earn/earnings04text.zip" , "https://www.ssa.gov/policy/docs/microdata/epuf/epuf2006_csv_files.zip" , "https://www.ssa.gov/policy/docs/microdata/mbr/mbr_csv.zip" , "https://www.ssa.gov/policy/docs/microdata/ssr/ssr_csv.zip" )
		
		catalog <- catalog[ !( catalog$full_url %in% duplicates ) , ]

		catalog$output_folder <- paste0( output_dir , "/" , gsub( "\\.zip$" , "" , basename( catalog$full_url) , ignore.case = TRUE ) )
		
		catalog

	}


lodown_ssa <-
	function( data_name = "ssa" , catalog , ... ){

		on.exit( print( catalog ) )

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			# download the dictionary
			download.file( catalog[ i , "doc_url" ] , tf , mode = 'wb' )

			file.copy( tf , paste0( catalog[ i , 'output_folder' ] , "/" , basename( catalog[ i , 'doc_url' ] ) ) )
			
			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' , filesize_fun = 'unzip_verify' )

			unzipped_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , "/unzips" ) )

			for( this_file in unzipped_files[ !grepl( "\\.pdf$" , unzipped_files , ignore.case = TRUE ) ] ){
			
				x <- data.frame( haven::read_sas( this_file ) )
					
				# convert all column names to lowercase
				names( x ) <- tolower( names( x ) )

				catalog[ i , 'case_count' ] <- max( catalog[ i , 'case_count' ] , nrow( x ) , na.rm = TRUE )
				
				saveRDS( x , file = paste0( catalog[ i , 'output_folder' ] , "/" , gsub( "\\.sas7bdat" , ".rds" , basename( this_file ) , ignore.case = TRUE ) ) , compress = FALSE )

			}
			
			# delete the temporary files
			suppressWarnings( file.remove( tf , unzipped_files ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'output_folder' ] , "'\r\n\n" ) )

		}

		on.exit()
		
		catalog

	}

