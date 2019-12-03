get_catalog_mapdcrosswalk <-
  function( data_name = "mapdcrosswalk" , output_dir , ... ){

	cpsc_url <- "https://www.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/MCRAdvPartDEnrolData/Plan-Crosswalks.html"

	all_dates <- rvest::html_table(xml2::read_html(cpsc_url))

	all_dates <- all_dates[[1]][ , "Report Period"]

	all_links <- rvest::html_nodes(xml2::read_html(cpsc_url),xpath='//td/a')

	prefix <- "https://www.cms.gov/"

	these_links <- 
		gsub( "\">(.*)" , "" , 
		gsub( '<a href=\"' , prefix , 
			all_links ) )

    this_catalog <-
      data.frame(
          output_filename = paste0( output_dir , "/" , all_dates , " plan crosswalk.rds" ) ,
          full_url = as.character( these_links ) ,
          year = all_dates ,
		  stringsAsFactors = FALSE
      )
	
	for( this_row in seq( nrow( this_catalog ) ) ){
		
		link_text <- readLines( this_catalog[ this_row , 'full_url' ] )
		link_line <- grep( "walk(.*)zip|zip(.*)walk" , link_text , value = TRUE )
		link_line <- gsub( '(.*) href=\"' , "" , gsub( '(.*) href=\"/' , prefix , link_line ) )
		this_catalog[ this_row , 'full_url' ] <- gsub( '\">(.*)' , "" , link_line )

	}
	
	
	this_catalog[ order( this_catalog$year ) , ]
  }


lodown_mapdcrosswalk <-
  function( data_name = "mapdcrosswalk" , catalog , ... ){

	on.exit( print( catalog ) )

    tf <- tempfile()

    for ( i in seq_len( nrow( catalog ) ) ){

		# download the file
		cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )


		# extract the contents of the zipped file
		# into the current year-month-specific directory
		# and (at the same time) create an object called
		# `unzipped_files` that contains the paths on
		# your local computer to each of the unzipped files
		unzipped_files <- unzip_warn_fail( tf , exdir = np_dirname( catalog[ i , 'output_filename' ] ) )

		x <- NULL
		
		for( this_fn in grep( "\\.xls" , unzipped_files , value = TRUE ) ) x <- rbind( x , data.frame( readxl::read_excel( this_fn ) ) )
		
		x <- unique( x )
		
		x$year <- catalog[ i , 'year' ]
		
		# convert all column names to lowercase
		names( x ) <- tolower( names( x ) )
		
		saveRDS( x , file = catalog[ i , 'output_filename' ] , compress = FALSE )

		# add the number of records to the catalog
		catalog[ i , 'case_count' ] <- nrow( x )

		# delete the temporary files
		file.remove( tf , unzipped_files )

		cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

    }

	on.exit()
	
    catalog

  }

