get_catalog_mics <-
	function( data_name = "mics" , output_dir , ... ){

		if ( !requireNamespace( "jsonlite" , quietly = TRUE ) ) stop( "jsonlite needed for this function to work. to install it, type `install.packages( 'jsonlite' )`" , call. = FALSE )

		if( !( 'your_email' %in% names(list(...)) ) ) stop( "`your_email` parameter must be specified.  create an account at http://mics.unicef.org/visitors/sign-up" )
		
		your_email <- list(...)[["your_email"]]

		if( !( 'your_password' %in% names(list(...)) ) ) stop( "`your_password` parameter must be specified.  create an account at http://mics.unicef.org/visitors/sign-up" )
		
		your_password <- list(...)[["your_password"]]

		mics_authenticate( your_email , your_password )

		res <- httr::GET( "http://mics.unicef.org/api/survey" )
	
		
		catalog <- data.frame( jsonlite::fromJSON( httr::content( res , as = "text" ) , flatten = TRUE ) )
		
		catalog <- catalog[ catalog$dataset.url != "" , ]
		
		catalog$dataset.status <- NULL
		
		names( catalog )[ names( catalog ) == 'dataset.url' ] <- 'full_url'
		
		catalog$output_folder <- paste0( output_dir , "/" , catalog$country , "/" , catalog$year )
		
		catalog

	}


lodown_mics <-
	function( data_name = "mics" , catalog , ... ){

		on.exit( print( catalog ) )

		if ( !requireNamespace( "jpeg" , quietly = TRUE ) ) stop( "jpeg needed for this function to work. to install it, type `install.packages( 'jpeg' )`" , call. = FALSE )

		if( !( 'your_email' %in% names(list(...)) ) ) stop( "`your_email` parameter must be specified.  create an account at http://mics.unicef.org/visitors/sign-up" )
		
		your_email <- list(...)[["your_email"]]

		if( !( 'your_password' %in% names(list(...)) ) ) stop( "`your_password` parameter must be specified.  create an account at http://mics.unicef.org/visitors/sign-up" )
		
		your_password <- list(...)[["your_password"]]

		mics_authenticate( your_email , your_password )

		tf <- tempfile()
		
		for ( i in seq_len( nrow( catalog ) ) ){

			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

			unzipped_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , "/unzips" ) )

			for( this_zip in grep( "\\.zip$" , unzipped_files , value = TRUE , ignore.case = TRUE ) ) unzipped_files <- c( unzipped_files , unzip_warn_fail( this_zip , exdir = paste0( tempdir() , "/unzips" ) ) )
							
			sav_files <- grep( "\\.sav$" , unzipped_files , ignore.case = TRUE , value = TRUE )
			
			if( length( sav_files ) == 0 ) stop( "zero files to import" )
			
			for( this_sav in sav_files ){
			
				x <- data.frame( haven::read_spss( this_sav ) )

				# convert all column names to lowercase
				names( x ) <- tolower( names( x ) )

				catalog[ i , 'case_count' ] <- max( catalog[ i , 'case_count' ] , nrow( x ) , na.rm = TRUE )
				
				saveRDS( x , file = paste0( catalog[ i , 'output_folder' ] , "/" , gsub( "\\.sav" , ".rds" , basename( this_sav ) , ignore.case = TRUE ) ) , compress = FALSE )
				
			}

			# delete the temporary files
			suppressWarnings( file.remove( tf , unzipped_files ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'output_folder' ] , "'\r\n\n" ) )

		}

		on.exit()
		
		catalog

	}


	
	
mics_authenticate <-
	function( your_email , your_password ){
		
		tf <- tempfile() ; tf2 <- tempfile()

		signin_page <- "http://mics.unicef.org/visitors/sign-in"
			
		a <- httr::GET( signin_page )

		writeBin( a$content , tf )

		if( any( grepl( "Log Out" , readLines(tf) ) ) ) return( invisible( TRUE ) )
		
		readline( prompt = "" )

		this_page <- readLines( tf ) 

		this_token <- gsub( '(.*)authenticity_token\" type=\"hidden\" value=\"(.*)\"(.*)' , "\\2" , grep( "authenticity_token" , this_page , value = TRUE ) )

		challenge_page <- gsub( '<script type=\"text/javascript\" src=\"(.*)\"(.*)' , "\\1" , grep("recaptcha",this_page , value = TRUE )[1] )

		challenge_res <- httr::GET( paste0( "https:" , challenge_page ) )

		suppressMessages( chal <- gsub( "(.*)challenge : '(.*)',\n    timeout(.*)" , "\\2" , httr::content(challenge_res) ) )

		b <- paste0( "https://www.google.com/recaptcha/api/image?c=" , chal )

		suppressMessages( download.file( b , tf2 , mode = 'wb' ) )

		jj <- jpeg::readJPEG(tf2,native=TRUE)

		plot(0:1,0:1,type="n",ann=FALSE,axes=FALSE)

		rasterImage(jj,0,0,1,.57/3)

		captcha_value <- readline( prompt = "a captcha image just popped up.  type the text of the image here: " )


		values <-
			list(
				authenticity_token = this_token ,
				utf8 = "&#x2713;" ,
				"visitor[email]" = your_email ,
				"visitor[password]" = your_password ,
				recaptcha_challenge_field = chal ,
				recaptcha_response_field = captcha_value ,
				commit = "Log in"
			)

			

		b <- httr::POST( signin_page , body = values )

		writeBin(b$content,tf)

		if( !any( grepl( "Log Out" , readLines(tf) ) ) ) stop( "you are not logged in" ) else invisible( TRUE )
	
	}
	