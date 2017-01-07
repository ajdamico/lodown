get_catalog_pew <-
	function( data_name = "pew" , output_dir , ... ){
			
		catalog <- NULL

		dl_homepage <- "http://www.pewresearch.org/data/download-datasets/"

		# figure out research areas #
		research_areas <- xml2::read_html( dl_homepage )

		ra_link_refs <- rvest::html_attr( rvest::html_nodes( research_areas , "a" ) , "href" )
			
		ra_link_text <- rvest::html_text( rvest::html_nodes( research_areas , "a" ) )

		ra_link_text <- ra_link_text[ grep( "/datasets/" , ra_link_refs ) ]

		ra_link_refs <- ra_link_refs[ grep( "/datasets/" , ra_link_refs ) ]


		for( topic_num in seq_along( ra_link_text ) ){

			# figure out years #
			topic_page <- xml2::read_html( ra_link_refs[ topic_num ] )

			to_link_refs <- rvest::html_attr( rvest::html_nodes( topic_page , "a" ) , "href" )
				
			to_link_text <- rvest::html_text( rvest::html_nodes( topic_page , "a" ) )

			year_link_refs <- to_link_refs[ grep( "^[0-9][0-9][0-9][0-9]$" , to_link_text ) ]

			year_link_text <- to_link_text[ grep( "^[0-9][0-9][0-9][0-9]$" , to_link_text ) ]

			for( year_num in seq_along( year_link_text ) ){

				# figure out pages #
				year_page <- xml2::read_html( year_link_refs[ year_num ] )

				data_link_refs <- rvest::html_attr( rvest::html_nodes( year_page , "a" ) , "href" )
					
				data_link_text <- rvest::html_text( rvest::html_nodes( year_page , "a" ) )

				page_list <- as.numeric( unique( gsub( "(.*)/page/([0-9]+)/$" , "\\2" , grep( "/page/[0-9]+/$" , data_link_refs , value = TRUE ) ) ) )
				
				if( length( page_list ) == 0 ) all_pages <- 1 else all_pages <- seq( max( page_list ) )

				
				for( page_num in all_pages ){

					# figure out datasets #
					these_data_webpage <- paste0( year_link_refs[ year_num ] , "page/" , page_num , "/" )

					these_data_page <- xml2::read_html( these_data_webpage )

					these_data_link_refs <- rvest::html_attr( rvest::html_nodes( these_data_page , "a" ) , "href" )
						
					these_data_link_text <- rvest::html_text( rvest::html_nodes( these_data_page , "a" ) )


					these_data_text <- these_data_link_text[ grep( "\\?download=[0-9]+$" , these_data_link_refs ) ]

					these_data_text <- lapply( strsplit( these_data_text , "\n" ) , function( z ) { a <- stringr::str_trim( z ) ; a[ a!='' ] } )
					
					these_data_refs <- these_data_link_refs[ grep( "\\?download=[0-9]+$" , these_data_link_refs ) ]

					these_data_info <- if( all( sapply( these_data_text , length ) >= 2 ) ) sapply( these_data_text , "[[" , 2 ) else NA
					
					this_catalog <-
						data.frame(
							full_url = paste0( year_link_refs[ year_num ] , these_data_refs ) ,
							name = sapply( these_data_text , '[[' , 1 ) ,
							download_id = gsub( "(.*)\\?download=" , "" , these_data_refs ) ,
							year = year_link_text[ year_num ] ,
							topic = ra_link_text[ topic_num ] ,
							info = these_data_info ,
							stringsAsFactors = FALSE
						)
						
					catalog <- rbind( catalog , this_catalog )
					
					cat( paste0( "loading " , data_name , " catalog from " , these_data_webpage , "\r\n\n" ) )

				}

			}

		}

		catalog$output_folder <- paste0( output_dir , "/" , catalog$topic , "/" , catalog$year , "/" , catalog$name , "/" )
		
		catalog

	}


lodown_pew <-
	function( data_name = "pew" , catalog , ... ){

		tf <- tempfile()

		
		if( !( 'your_name' %in% names(list(...)) ) ) stop( paste0( "`your_name` parameter must be specified.  review terms at " , catalog[ 1 , "full_url" ] ) )
		if( !( 'your_org' %in% names(list(...)) ) ) stop( paste0( "`your_org` parameter must be specified.  review terms at " , catalog[ 1 , "full_url" ] ) )
		if( !( 'your_phone' %in% names(list(...)) ) ) stop( paste0( "`your_phone` parameter must be specified.  review terms at " , catalog[ 1 , "full_url" ] )  )
		if( !( 'your_email' %in% names(list(...)) ) ) stop( paste0( "`your_email` parameter must be specified.  review terms at " , catalog[ 1 , "full_url" ] ) )
		if( !( 'agree_to_terms' %in% names(list(...)) ) ) stop( paste0( "`agree_to_terms` parameter must be `TRUE`.  review terms at " , catalog[ 1 , "full_url" ] ) )

		your_name <- list(...)[["your_name"]]
		your_org <- list(...)[["your_org"]]
		your_phone <- list(...)[["your_phone"]]
		your_email <- list(...)[["your_email"]]
		agree_to_terms <- list(...)[["agree_to_terms"]]

		authentication_list <-
			list( 
				Name=your_name , 
				Title= "" , 
				Organization=your_org , 
				Address1="" , 
				Address2="" ,
				City="" ,
				State="" , 
				Zip="", 
				Phone=your_phone , 
				Email=your_email , 
				Human="" , 
				Agreement=if(agree_to_terms)"Y"else"N" , 
				Action="Post" 
			)
		
		for ( i in seq_len( nrow( catalog ) ) ){

			this_valid_url <- paste0( catalog[ i , "full_url" ] , "&valid=y" )
			
			this_authentication_list <- c( authentication_list , list( download_id = catalog[ i , "download_id" ] ) )
			
			resp <- httr::POST( this_valid_url , body = this_authentication_list )
		
			this_file <- cachaca( resp$url , FUN = httr::GET )
			
			writeBin( this_file$content , tf )

			unzipped_files <- unzip( tf , exdir = catalog[ i , "output_folder" ] )

			sav_file <- grep( "\\.sav$" , unzipped_files , ignore.case = TRUE , value = TRUE )
			
			x <- data.frame( haven::read_spss( sav_file ) )

			# convert all column names to lowercase
			names( x ) <- tolower( names( x ) )

			save( x , file = gsub( "\\.sav$" , ".rda" , sav_file , ignore.case = TRUE ) )

			# delete the temporary files
			suppressWarnings( file.remove( tf ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

		}

		invisible( TRUE )

	}

