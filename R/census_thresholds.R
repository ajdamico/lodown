
census_thresholds <-
	function(){
	
		if ( !requireNamespace( "reshape2" , quietly = TRUE ) ) stop( "reshape2 needed for this function to work. to install it, type `install.packages( 'reshape2' )`" , call. = FALSE )
		if ( !requireNamespace( "readxl" , quietly = TRUE ) ) stop( "readxl needed for this function to work. to install it, type `install.packages( 'readxl' )`" , call. = FALSE )

		
		# initiate an empty data.frame object
		all_thresholds <- NULL

		cpov <- "https://www.census.gov/data/tables/time-series/demo/income-poverty/historical-poverty-thresholds.html" 

		pgdl <- try( pg <- xml2::read_html(cpov) , silent = TRUE )
		if( 'try-error' %in% class( pgdl ) ) pg <- xml2::read_html(cpov, method='wininet')

		all_links <- rvest::html_attr( rvest::html_nodes(pg, "a"), "href")

		# find all excel files on the census poverty webpage
		excel_locations <- grep( "thresholds/thresh(.*)\\.(.*)" , all_links , value = TRUE )

		# figure out which years are available among the excel file strings
		ya <- ( 1990:2058 )[ substr( 1990:2058 , 3 , 4 ) %in% gsub( "(.*)thresh(.*)\\.(.*)" , "\\2" , excel_locations ) ]

		# loop through all years available
		for ( year in rev(ya) ){

			# figure out the location of the excel file on the drive
			this_excel <- unique( paste0( "https:" , grep( substr( year , 3 , 4 ) , excel_locations , value = TRUE ) ) )
			
			# name the excel file something appropriate
			fn <- paste0( tempdir() , "/" , basename( this_excel ) )
			
			# download the file to your local disk
			httr::GET( this_excel , httr::write_disk( fn , overwrite = TRUE ) )

			# import the current table
			if( grepl( "\\.csv$" , fn ) ){

				skipsix <- try( this_thresh <- read.csv( fn , skip = 6 , stringsAsFactors = FALSE ) , silent = TRUE )
				
				if( class( skipsix ) == 'try-error' ) skipfive <- try( this_thresh <- read.csv( fn , skip = 7 , stringsAsFactors = FALSE ) , silent = TRUE ) else skipfive <- NULL
				
				if( class( skipfive ) == 'try-error' ) this_thresh <- read.csv( fn , skip = 5 , stringsAsFactors = FALSE )
				
			} else {
				
				this_thresh <- data.frame( readxl::read_excel( fn ) )
				
			}
			
			# if the text `Weighted` exists in the second column, toss the second column
			if( any( grepl( "Weighted" , c( names( this_thresh )[2] , this_thresh[ , 2 ] ) ) ) ) this_thresh <- this_thresh[ , -2 ]
			
			# keep all rows where the second column is not missing
			this_thresh <- this_thresh[ !is.na( this_thresh[ , 2 ] ) & !( stringr::str_trim( this_thresh[ , 2 ] ) == "" ) , ]
			
			# remove crap at the beginning and end
			this_thresh[ , 1 ] <- stringr::str_trim( iconv( as.character( this_thresh[ , 1 ] ) , to = "ASCII" , sub = " " ) )
			this_thresh[ , 1 ] <- stringr::str_trim( gsub( "\\." , "" , as.character( this_thresh[ , 1 ] ) ) )
			this_thresh <- this_thresh[ !is.na( this_thresh[ , 1 ] ) & !( this_thresh[ , 1 ] %in% "" ) ,  ]
			
			this_thresh[ -1 ] <- sapply( this_thresh[ -1 ] , function( z ) as.numeric( gsub( ",|\\$" , "" , z ) ) )
			
			
			# keep only rows where a `family_type` matches something we've already found
			# this_thresh <- this_thresh[ this_thresh[ , 1 ] %in% all_thresholds$family_type , ]

			# name the 2nd-10th columns based on number of kids
			names( this_thresh ) <- c( "family_type" , 0:8 )
			
			# reshape the table from wide to long
			this_thresh <- reshape2::melt( this_thresh , "family_type" )
			
			# appropriately name everything
			names( this_thresh ) <- c( 'family_type' , 'num_kids' , 'threshold' )
			
			# tack on the year
			this_thresh$year <- year
			
			this_thresh <- this_thresh[ !is.na( this_thresh$family_type ) & !is.na( this_thresh$threshold ) , ]
				
			# typo on census bureau page
			this_thresh <- this_thresh[ !( this_thresh$threshold == 26753 & this_thresh$year == 2000 & this_thresh$num_kids == 8 & this_thresh$family_type == "Eight persons" ) , ]

			stopifnot( nrow( this_thresh ) == 48 )
			
			# stack it with the others
			all_thresholds <- rbind( all_thresholds , this_thresh )
			
		}

		all_thresholds$family_type <- gsub( "persons" , "people" , all_thresholds$family_type )
		all_thresholds$num_kids <- as.numeric( as.character( all_thresholds$num_kids ) )

		all_thresholds$family_type <-
			gsub( "aged 65 and older" , "65 years and over" ,
			gsub( "age 65" , "65 years" , 
				all_thresholds$family_type , 
				ignore.case = TRUE ) , 
				ignore.case = TRUE )
		
		all_thresholds

		# done scraping official census poverty thresholds back to 1990
	}
	