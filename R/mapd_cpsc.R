get_catalog_mapd_cpsc <-
  function( data_name = "mapd_cpsc" , output_dir , ... ){

	cpsc_url <- "https://www.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/MCRAdvPartDEnrolData/Monthly-Enrollment-by-Contract-Plan-State-County.html"

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
          output_filename = paste0( output_dir , "/" , all_dates , " cpsc enrollment.rds" ) ,
          full_url = as.character( these_links ) ,
          year_month = all_dates ,
		  stringsAsFactors = FALSE
      )
	
	for( this_row in seq( nrow( this_catalog ) ) ){
		
		link_text <- readLines( this_catalog[ this_row , 'full_url' ] )
		link_line <- grep( "zip" , link_text , value = TRUE )
		link_line <- gsub( '(.*) href=\"' , "" , gsub( '(.*) href=\"/' , prefix , link_line ) )
		this_catalog[ this_row , 'full_url' ] <- gsub( '\">(.*)' , "" , link_line )

	}
	
	
	this_catalog[ order( this_catalog$year_month ) , ]
  }


lodown_mapd_cpsc <-
  function( data_name = "mapd_cpsc" , catalog , ... ){

    tf <- tempfile()


    for ( i in seq_len( nrow( catalog ) ) ){

		# download the file
		cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' , filesize_fun = 'httr' )


		# extract the contents of the zipped file
		# into the current year-month-specific directory
		# and (at the same time) create an object called
		# `unzipped_files` that contains the paths on
		# your local computer to each of the unzipped files
		unzipped_files <- unzip_warn_fail( tf , exdir = np_dirname( catalog[ i , 'output_filename' ] ) )

		this_cont <- data.frame( readr::read_csv( grep( "Contract_Info" , unzipped_files , value = TRUE ) , guess_max = 100000 ) )

		this_enr <- data.frame( readr::read_csv( grep( "Enrollment_Info" , unzipped_files , value = TRUE ) , guess_max = 100000 ) )

		names( this_cont ) <- gsub( "Contract.Number" , "Contract.ID" , names( this_cont ) , fixed = TRUE )
		names( this_enr ) <- gsub( "Contract.Number" , "Contract.ID" , names( this_enr ) , fixed = TRUE )
		names( this_enr ) <- gsub( "SSA.Code" , "SSA.State.County.Code" , names( this_enr ) , fixed = TRUE )
		names( this_enr ) <- gsub( "FIPS.Code" , "FIPS.State.County.Code" , names( this_enr ) , fixed = TRUE )

		this_cont$this_date <- catalog[ i , 'year_month' ]
		this_enr$this_date <- catalog[ i , 'year_month' ]
		
		this_enr$enrolled <- as.numeric( gsub( "," , "" , this_enr$Enrollment ) )
		
		this_enr$Plan.ID <- as.numeric( this_enr$Plan.ID )
		this_cont$Plan.ID <- as.numeric( this_cont$Plan.ID )
		
		this_enr <- this_enr[ , c( 'this_date' , 'Contract.ID' , 'Plan.ID' , 'FIPS.State.County.Code' , 'enrolled' ) ]

		this_enr <- unique( this_enr )
		
		this_cont <- unique( this_cont )
		
		x <- merge( this_cont , this_enr )
		
		stopifnot( nrow( x ) == nrow( this_enr ) )

		# convert all column names to lowercase
		names( x ) <- tolower( names( x ) )

		saveRDS( x , file = catalog[ i , 'output_filename' ] )

		# add the number of records to the catalog
		catalog[ i , 'case_count' ] <- nrow( x )

		# delete the temporary files
		file.remove( tf , unzipped_files )

		cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

    }

    catalog

  }

