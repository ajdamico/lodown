get_catalog_nsduh <-
	function( data_name = "nsduh" , output_dir , ... ){

		series_xml <- xml2::read_html( "http://datafiles.samhsa.gov/info/browse-studies-nid3454" )
		
		links <- grepl( "nsduh|nhsda" , rvest::html_nodes( series_xml , "a" ) )
		
		# remove 1994 and 2002-2014 and 2002-2015 files
		links <- links & !grepl( "1994|2002-201" , rvest::html_text( rvest::html_nodes( series_xml , "a" ) ) )
		
		link_text <- rvest::html_text( rvest::html_nodes( series_xml , "a" ) )[ links ]
		link_year <- gsub( "(.*)-|\\r(.*)" , "" , link_text )
	
		link_url <- paste0( "http://datafiles.samhsa.gov" , rvest::html_attr( rvest::html_nodes( series_xml , "a" ) , 'href' )[ links ] )

		for( this_link in seq( link_url ) ){
		
			this_xml <- xml2::read_html( link_url[ this_link ] )

			this_study_page <-
				paste0( 
					"http://datafiles.samhsa.gov" , 
					grep( "ds0001" , rvest::html_attr( rvest::html_nodes( this_xml , "a" ) , 'href' ) , value = TRUE )
				)
				
			stopifnot( length( this_study_page ) == 1 )
		
			
			this_page <- xml2::read_html( this_study_page )

			this_file <- grep( "data-stata\\.zip" , rvest::html_attr( rvest::html_nodes( this_page , "a" ) , 'href' ) , value = TRUE )
			
			stopifnot( length( this_file ) == 1 )
			
			link_url[ this_link ] <- this_file
			
		}
		
		catalog <-
			data.frame(
				year = link_year ,
				full_url = link_url ,
				output_filename = paste0( output_dir , "/" , link_year , " main.rds" ) ,
				unzip_folder = paste0( output_dir , "/" , link_year , "/" ) ,
				stringsAsFactors = FALSE
			)
			
		# add 1994 A and B
		catalog <-
			rbind( 
				catalog ,
				data.frame(
					year = c( '1994' , '1994' ) ,
					full_url = 
						c( "http://samhda.s3-us-gov-west-1.amazonaws.com/s3fs-public/field-uploads-protected/studies/NHSDA-1994/NHSDA-1994-datasets/NHSDA-1994-DS0001/NHSDA-1994-DS0001-bundles-with-study-info/NHSDA-1994-DS0001-bndl-data-stata.zip" ,
						"http://samhda.s3-us-gov-west-1.amazonaws.com/s3fs-public/field-uploads-protected/studies/NHSDA-1994/NHSDA-1994-datasets/NHSDA-1994-DS0002/NHSDA-1994-DS0002-bundles-with-study-info/NHSDA-1994-DS0002-bndl-data-stata.zip"
						),
					output_filename = paste0( output_dir , "/1994 " , c( "part a" , "part b" ) , ".rds" ) ,
					unzip_folder = paste0( output_dir , "/1994 " , c( "a" , "b" ) , "/" ) ,
					stringsAsFactors = FALSE
				)
			)
	
		catalog[ order( catalog$year ) , ]

}


lodown_nsduh <-
	function( data_name = "nsduh" , catalog , ... ){

		on.exit( print( catalog ) )
	
		tf <- tempfile()
	
		for( i in seq_len( nrow( catalog ) ) ){
		
			cachaca( catalog[ i , 'full_url' ] , tf , mode = 'wb' )
			
			unzip_warn_fail( tf , exdir =  gsub( "/$" , "" , catalog[ i , "unzip_folder" ] ) , junkpaths = TRUE )
			
			file.remove( tf )
		
			# find stata file within unzipped path
			stata_files <- grep( "\\.dta$" , list.files( catalog[ i , 'unzip_folder' ] , full.names = TRUE ) , value = TRUE )
			
			stopifnot( length( stata_files ) == 1 )
			
			x <- icpsr_stata( stata_files , catalog_entry = catalog[ i , ] )

			names( x ) <- tolower( names( x ) )
			
			catalog[ i , 'case_count' ] <- nrow( x )
			
			saveRDS( x , file = catalog[ i , 'output_filename' ] ) ; rm( x ) ; gc()
			
			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )
		
		}

		on.exit()
		
		catalog

	}

