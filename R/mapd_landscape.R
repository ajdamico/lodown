get_catalog_mapd_landscape <-
	function( data_name = "mapd_landscape" , output_dir , ... ){

		landscape_url <- "https://www.cms.gov/Medicare/Prescription-Drug-Coverage/PrescriptionDrugCovGenIn/index.html?redirect=/PrescriptionDrugCovGenIn/"

		prefix <- "https://www.cms.gov/"
		
		all_links <- rvest::html_nodes( xml2::read_html( landscape_url ) , xpath = '//li/a' )

		all_names <- rvest::html_text( all_links )
	
		all_links <- gsub( '(.*)href=\"' , prefix , all_links )
		all_links <- gsub( "\">(.*)" , "" , all_links )

		zipped_link_nums <- grep( "\\.zip$" , all_links , ignore.case = TRUE )
		
		zip_links <- all_links[ zipped_link_nums ]
		zip_names <- gsub( "( +?)\\t(.*)" , "" , all_names[ zipped_link_nums ] )
		
		
		this_catalog <-
		  data.frame(
			  output_folder = paste0( output_dir , "/" , zip_names ) ,
			  full_url = zip_links ,
			  stringsAsFactors = FALSE
		  )
	
		this_catalog
	}


lodown_mapd_landscape <-
	function( data_name = "mapd_landscape" , catalog , ... ){

		tf <- tempfile()


		for ( i in seq_len( nrow( catalog ) ) ){

			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' , filesize_fun = 'httr' )


			unzipped_files <- unzip_warn_fail( tf , exdir = catalog[ i , 'output_folder' ] )

			
			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'output_folder' ] , "'\r\n\n" ) )

		}

		catalog

	}

