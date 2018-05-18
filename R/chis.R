get_catalog_chis <-
	function( data_name = "chis" , output_dir , ... ){

		if( !( 'your_username' %in% names(list(...)) ) ) stop( "`your_username` parameter must be specified.  create an account at http://healthpolicy.ucla.edu/pages/login.aspx" )
		
		your_username <- list(...)[["your_username"]]

		if( !( 'your_password' %in% names(list(...)) ) ) stop( "`your_password` parameter must be specified.  create an account at http://healthpolicy.ucla.edu/pages/login.aspx" )
		
		your_password <- list(...)[["your_password"]]

		this_curl <- chis_authenticate( your_username , your_password )

		data_page <- RCurl::getURL( "http://healthpolicy.ucla.edu/chis/data/public-use-data-file/Pages/public-use-data-files.aspx" , curl = this_curl )
		
		year_lines <- substr( strsplit( data_page , "/chis/data/public-use-data-file/Pages/" , fixed = TRUE )[[1]] , 1 , 10 )
		
		suppressWarnings( {
			first_two_years <- as.numeric( gsub( '(.*)([0-9][0-9][0-9][0-9])-([0-9][0-9][0-9][0-9])(.*)' , "\\2" , year_lines ) )
			second_two_years <- as.numeric( gsub( '(.*)([0-9][0-9][0-9][0-9])-([0-9][0-9][0-9][0-9])(.*)' , "\\3" , year_lines ) )
			one_years <- as.numeric( gsub( '(.*)([0-9][0-9][0-9][0-9])(.*)' , "\\2" , year_lines ) )
		} )
		
		available_years <- unique( c( first_two_years , second_two_years , one_years ) )
		available_years <- sort( available_years[ !is.na( available_years ) ] )
		
		catalog <- expand.grid( available_years , c( 'adult' , 'teen' , 'child' ) , stringsAsFactors = FALSE )
		
		names( catalog ) <- c( 'year' , 'type' )
		
		catalog$full_url <- paste0( "http://healthpolicy.ucla.edu/chis/data/public-use-data-file/Documents/chis" , substr( catalog$year , 3 , 4 ) , "_" , catalog$type , "_stata.zip" )
		
		catalog[ catalog$year == 2014 & catalog$type == 'teen' , 'full_url' ] <- gsub( 'teen' , 'adolescent' , catalog[ catalog$year == 2014 & catalog$type == 'teen' , 'full_url' ] )
		
		catalog$output_filename <- paste0( output_dir , "/" , catalog$year , " " , catalog$type , ".rds" )
		
		catalog

	}


lodown_chis <-
	function( data_name = "chis" , catalog , ... ){

		on.exit( print( catalog ) )

		if( !( 'your_username' %in% names(list(...)) ) ) stop( "`your_username` parameter must be specified.  create an account at http://healthpolicy.ucla.edu/pages/login.aspx" )
		
		your_username <- list(...)[["your_username"]]

		if( !( 'your_password' %in% names(list(...)) ) ) stop( "`your_password` parameter must be specified.  create an account at http://healthpolicy.ucla.edu/pages/login.aspx" )
		
		your_password <- list(...)[["your_password"]]

		this_curl <- chis_authenticate( your_username , your_password )

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			
			# download the file number
			this_file <- cachaca( catalog[ i , 'full_url' ] , destfile = NULL , FUN = RCurl::getBinaryURL , curl = this_curl )

			# write the file to the temporary file on the local disk
			writeBin( this_file , tf )

			unzipped_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , "/unzips" ) )

			dta_file <- grep( "\\.dta$" , unzipped_files , value = TRUE )
			
			for( this_dta in dta_file ){
				
				if( grepl( "f\\.dta" , this_dta , ignore.case = TRUE ) ) savename <- gsub( "\\.rds" , "f.rds" , catalog[ i , 'output_filename' ] ) else savename <- catalog[ i , 'output_filename' ]
				
				if( file.exists( savename ) ) stop( "rds file already exists. delete the contents of your output_dir= and try again" )
				
				# load the .dta file as an R `data.frame` object
				x <- data.frame( haven::read_dta( this_dta ) )
				
				# convert all column names to lowercase
				names( x ) <- tolower( names( x ) )
				
				if( !grepl( "f\\.dta" , this_dta , ignore.case = TRUE ) ) catalog[ i , 'case_count' ] <- nrow( x )
				
				# store the `data.frame` object as an .rds file on the local disk
				saveRDS( x , file = savename , compress = FALSE )
				
			}
				
			# delete the temporary files
			suppressWarnings( file.remove( tf , unzipped_files ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

		}

		on.exit()
		
		catalog

	}




chis_authenticate <-
	function( your_username , your_password ){

		# initiate and then set a curl handle to store information about this download
		curl = RCurl::getCurlHandle()

		# initate a cookies file
		RCurl::curlSetOpt(
			cookiejar = 'cookies.txt' , 
			followlocation = TRUE , 
			autoreferer = TRUE , 
			curl = curl
		)

		# connect to the login page to download the contents of the `viewstate` option
		html <- RCurl::getURL( "http://healthpolicy.ucla.edu/pages/login.aspx" , curl = curl )

		# extract the `viewstate` string
		viewstate <- 
			as.character(
				sub(
					'.*id="__VIEWSTATE" value="([0-9a-zA-Z+/=]+)".*' , 
					'\\1' , 
					html
				)
			)
			
		viewstategenerator <- 
			as.character(
				sub(
					'.*id="__VIEWSTATEGENERATOR" value="([0-9a-zA-Z+/=]+)".*' , 
					'\\1' , 
					html
				)
			)
			
		# extract the `eventvalidation` string
		eventvalidation <- 
			as.character(
				sub(
					'.*id="__EVENTVALIDATION" value="([0-9a-zA-Z+/=]+)".*' , 
					'\\1' , 
					html
				)
			)

		requestdigest <-
			as.character(
				sub(
					'.*id="__REQUESTDIGEST" value="([0-9a-zA-Z+/=:\\, \\-]+)".*' , 
					'\\1' , 
					html
				)
			)
			
		# construct a list full of parameters to pass to the ucla website
		params <- 
			list(
				'ctl00$ctl29$g_3a8b961a_097a_4aa2_a7b2_9959a01104bd$ctl00$UserName'    = your_username ,
				'ctl00$ctl29$g_3a8b961a_097a_4aa2_a7b2_9959a01104bd$ctl00$Password'    = your_password ,
				'ctl00$ctl29$g_3a8b961a_097a_4aa2_a7b2_9959a01104bd$ctl00$LoginLinkButton' = TRUE ,
				'__VIEWSTATE'                                  = viewstate ,
				'__EVENTVALIDATION'                            = eventvalidation ,
				'__REQUESTDIGEST'							   = requestdigest ,
				'__VIEWSTATEGENERATOR'						   = viewstategenerator ,
				'__EVENTTARGET' = '' ,
				'__EVENTARGUMENT' = '' ,
				'__LASTFOCUS' = '' ,
				'MSOWebPartPage_PostbackSource' = '' ,
				'MSOTlPn_SelectedWpId' = '' ,
				'MSOTlPn_View' = '0' ,
				'MSOTlPn_ShowSettings' = 'False' ,
				'MSOGallery_SelectedLibrary' = '' ,
				'MSOGallery_FilterString' = '' ,
				'MSOTlPn_Button' = 'none' ,
				'MSOSPWebPartManager_DisplayModeName' = 'Browse' ,
				'MSOSPWebPartManager_ExitingDesignMode' = 'false' ,
				'MSOWebPartPage_Shared' = '' ,
				'MSOLayout_LayoutChanges' = '' ,
				'MSOLayout_InDesignMode' = '' ,
				'_wpSelected' = '' ,
				'_wzSelected' = '' ,
				'MSOSPWebPartManager_OldDisplayModeName' = 'Browse' ,
				'MSOSPWebPartManager_StartWebPartEditingName' = 'false' ,
				'MSOSPWebPartManager_EndWebPartEditing' = 'false'
			)
			
			
			
			
			
		# post these parameters to the login page to authenticate
		html = RCurl::postForm( "http://healthpolicy.ucla.edu/pages/login.aspx" , .params = params, curl = curl)

		# confirms the result's contents contains the word `Logout` because
		# if it does not contain this text, you're not logged in.  sorry.
		if ( !grepl('Logout', html) ) stop( 'YOU ARE NOT LOGGED IN' )

		curl
	}
	
