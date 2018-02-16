get_catalog_nls <-
	function( data_name = "nls" , output_dir , ... ){

		catalog <- NULL
		
		data_page <- "https://www.nlsinfo.org/accessing-data-cohorts"
	
		link_page <- rvest::html_nodes( xml2::read_html( data_page ) , "a" )
		
		link_text <- rvest::html_text( link_page )
		
		link_refs <- rvest::html_attr( link_page , "href" )
		
		microdata_text <- stringr::str_trim( link_text[ grep( "\\.zip$" , link_refs ) ] )
		
		microdata_refs <- stringr::str_trim( link_refs[ grep( "\\.zip$" , link_refs ) ] )
		
		catalog <-
			data.frame(
					study_name = microdata_text ,
					full_url = microdata_refs ,
					output_folder = paste0( output_dir , "/" , microdata_text , "/" ) ,
					stringsAsFactors = FALSE
				)
				
		catalog

	}


lodown_nls <-
	function( data_name = "nls" , catalog , path_to_7za = '7za' , ... ){

		on.exit( print( catalog ) )

		if( ( .Platform$OS.type != 'windows' ) && ( system( paste0('"', path_to_7za , '" -h' ) ) != 0 ) ) stop( "you need to install 7-zip.  if you already have it, include a path_to_7za='/directory/7za' parameter" )
 		
		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			# download the strata and psu for studies where they're available
			if( catalog[ i , 'study_name' ] == "NLS Youth 1997 (NLSY97)" ){
			
				# download the nlsy 1997 cohort's sampling information
				cachaca( "https://www.nlsinfo.org/sites/nlsinfo.org/files/attachments/140618/nlsy97stratumpsu.zip" , tf , mode = 'wb' )
				
				# unzip to the local disk
				unzipped_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , '/unzips' ) )

				strpsu <- read.csv( unzipped_files[ grep( '\\.csv' , unzipped_files ) ] , stringsAsFactors = FALSE )
				
				# store the complex sample variables on the local disk
				saveRDS( strpsu , file = paste0( catalog[ i , 'output_folder' ] , "/strpsu.rds" ) , compress = FALSE )
				
				# delete the temporary files
				suppressWarnings( file.remove( tf , unzipped_files ) )
				
			}

			cachaca( catalog[ i , 'full_url' ] , tf , mode = 'wb' )
			
			# extract the file, platform-specific
			if ( .Platform$OS.type == 'windows' ){

				unzipped_files <- unzip_warn_fail( tf , exdir = file.path( catalog[ i , 'output_folder' ] , 'unzips' ) )

			} else {

				# build the string to send to the terminal on non-windows systems
				dos.command <- paste0( '"' , path_to_7za , '" x ' , tf , ' -o"' , file.path( catalog[ i , 'output_folder' ] , 'unzips' ) , '"' )
				system( dos.command )
				unzipped_files <- list.files( catalog[ i , 'output_folder' ] , full.names = TRUE , recursive = TRUE )

			}
			
			this_dat_file <- grep( "\\.dat$" , unzipped_files , ignore.case = TRUE , value = TRUE )
			
			catalog[ i , 'case_count' ] <- R.utils::countLines( this_dat_file )
			
			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'output_folder' ] , "'\r\n\n" ) )

		}

		on.exit()
		
		catalog

	}


	
	
# # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # #
# functions related to nlsy panel weight download #
# # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # #


# initiate a function to download all available survey-year selections for any of the nlsy studies
nls_get_selections <-
	function(
		study
		# study must be one of the options shown on https://www.nlsinfo.org/weights such as:
		# "nlsy97" , "nlsy79" , "nlscya" , "nlsym" , "nlsom" , "nlsyw" , "nlsmw"
	){
		
		
		# for a particular study's weights page, download the contents of the page
		z <- httr::GET( paste0( "https://www.nlsinfo.org/weights/" , study ) )

		# de-construct the html
		doc <- XML::htmlParse( z )
		
		# look for all `input` blocks
		opts <- XML::getNodeSet( doc , "//input" )
		
		# look for all `name` attributes within input blocks
		all.name.values <- sapply( opts , XML::xmlGetAttr , "name" )
		
		# find all text containing the letters `SURV`
		all.surveys <- unlist( all.name.values[ grep( "SURV" , all.name.values ) ] )

		# and here are your year choices
		all.surveys
	}

	
# initiate a function to download a specific combination of survey-year weights
# for one of the nlsy studies
	
	
# set uona = "NO" if you want to weight using
# "the respondents are in ALL of the selected years"

# set uona = "YES" if you want to weight using
# "the respondents are in ANY OR ALL of the selected years"

nls_get_weights <-
	function( 
		study , 
		# study must be one of the options shown on https://www.nlsinfo.org/weights such as:
		# "nlsy97" , "nlsy79" , "nlscya" , "nlsym" , "nlsom" , "nlsyw" , "nlsmw"

		uona , 
		
		selections 
	){
	
		
		# make contact with the weights page
		httr::GET( paste0( "https://www.nlsinfo.org/investigator/pages/search.jsp?s=" , toupper( study ) ) )
		httr::GET( paste0( "https://www.nlsinfo.org/weights/" , study ) )
		
		# initiate a `values` list containing the series of survey-year selections
		values <- as.list( rep( "1" , length( selections ) ) )
		# these are just ones.
	
		# rename each object within the list according to the survey-year
		names( values ) <- selections
	
		# add the use-or-not-and decision
		values[[ "USE_OR_NOT_AND" ]] <- uona

		# add a few form parameters that the server just expects, but never change.
		values[[ "form_id" ]] <- "weights_cohort_form"
		
		values[[ "op" ]] <- "Download"
		
		values[[ "tab-group-1" ]] <- 'years'

		values[[ "accept-charset" ]] <- "UTF-8"

		
		# determine the form-build-id
		bid <- httr::GET( paste0( "https://www.nlsinfo.org/weights/" , study ) , query = values )
		
		# de-construct the html
		doc <- XML::htmlParse( bid )
		
		# look for `input` blocks
		opts <- XML::getNodeSet( doc , "//input" )
		
		# find all `name` attributes within `input` blocks
		all.name.values <- sapply( opts , XML::xmlGetAttr , "name" )
		
		# find all `value` attributes within `input` blocks
		all.values <- sapply( opts , XML::xmlGetAttr , "value" )
		
		# determine the two form-build-id values
		form.build.id <- all.values[ all.name.values == 'form_build_id' ]
		
		# take the second form-build-id on the page
		values[[ "form_build_id" ]] <- form.build.id[ 2 ]

		# download the data
		x <- httr::POST( paste0( "https://www.nlsinfo.org/weights/" , study ) , body = values )

		# initiate a temporary file on the local disk
		tf <- tempfile()

		# save the zipped file contents on the local drive
		writeBin( httr::content( x , "raw" ) , tf )

		# unzip the file and store the filepath into the object `d`
		d <- unzip( tf )

		# determine the `.dat` file that's just been unzipped
		dat <- d[ grep( '.dat' , d , fixed = TRUE ) ]

		# read both columns into an R data.frame
		y <- read.table( dat , sep = " " , col.names = c( 'R0000100' , 'weight' ) )

		# delete the temporary file from the local disk
		unlink( tf )
		
		# delete all unzipped files from the local disk
		unlink( d )
		
		# return the data.frame containing the weights
		y
	}

