get_catalog_gss <-
	function( data_name = "gss" , output_dir , ... ){

		data_page <- "http://gss.norc.org/get-the-data/spss"
		
		data_html <- xml2::read_html( data_page )
		
		all_links <- rvest::html_nodes( data_html , "a" )
		
		link_text <- rvest::html_text( all_links )
		
		link_refs <- rvest::html_attr( all_links , "href" )
		
		zip_text <- link_text[ grep( "\\.zip$" , link_refs , ignore.case = TRUE ) ]
		
		zip_refs <- link_refs[ grep( "\\.zip$" , link_refs , ignore.case = TRUE ) ]
		
		zip_text <- tolower( gsub( "-|( +)" , " " , gsub( "\\(|\\)|,|\\." , "" , iconv( zip_text , "" , "ASCII" , sub = " " ) ) ) )
		
		catalog <-
			data.frame(
				output_filename = paste0( output_dir , "/" , zip_text , ".rds" ) ,
				full_url = paste0( "http://gss.norc.org/" , zip_refs ) ,
				stringsAsFactors = FALSE
			)
		
		# remove broken links
		catalog <- subset( catalog , !grepl( "panel06w123|2010merged_R1_spss1|gss_panel_W2.spss" , full_url ) )
		
		catalog

	}


lodown_gss <-
	function( data_name = "gss" , catalog , ... ){

		on.exit( print( catalog ) )

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

			unzipped_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , "/unzips" ) )

			stopifnot( length( this_sav <- grep( "\\.sav$" , unzipped_files , ignore.case = TRUE , value = TRUE ) ) == 1 )
			
			x <- data.frame( haven::read_spss( this_sav ) )

			x$one <- 1
			
			# convert all column names to lowercase
			names( x ) <- tolower( names( x ) )

			catalog[ i , 'case_count' ] <- nrow( x )
			
			saveRDS( x , file = catalog[ i , 'output_filename' ] , compress = FALSE ) ; rm( x ) ; gc()

			# delete the temporary files
			suppressWarnings( file.remove( tf , unzipped_files ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

		}

		on.exit()
		
		catalog

	}

