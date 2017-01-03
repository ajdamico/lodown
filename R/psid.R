get_catalog_psid <-
	function( data_name = "psid" , output_dir , ... ){

		catalog <- NULL

		zipsmain <- xml2::read_html( "http://simba.isr.umich.edu/Zips/ZipMain.aspx" )
		
		table_urls <- xml2::url_absolute( rvest::html_attr( rvest::html_nodes( zipsmain , ".rtIn" ) , "href") , "http://simba.isr.umich.edu/Zips/" )
		  
		table_text <- rvest::html_text( rvest::html_nodes( zipsmain , ".rtIn" ) )

		catalog <- data.frame( full_url = table_urls , table_name = table_text , directory = NA , stringsAsFactors = FALSE )
		
		catalog <- catalog[ !grepl( "(Coming Soon)" , catalog$table_name , fixed = TRUE ) , ]
		
		catalog <- catalog[ !grepl( "^[0-9][0-9][0-9][0-9]s$" , catalog$table_name ) , ]
		
		for( this_row in seq( nrow( catalog ) ) ) catalog[ this_row , 'directory' ] <- ifelse( grepl( "/NA$" , catalog[ this_row , 'full_url' ] ) , catalog[ this_row , 'table_name' ] , catalog[ this_row - 1 , 'directory' ] )
			
		catalog <- catalog[ !grepl( "/NA$" , catalog[ , 'full_url' ] ) , ]
				
		catalog$year = ifelse( grepl( "^[0-9][0-9][0-9][0-9]" , catalog$table_name ) , substr( catalog$table_name , 1 , 4 ) , NA )
		
		catalog$type <- ifelse( grepl( "^[0-9][0-9][0-9][0-9] Wealth$" , catalog$table_name ) , "Wealth Files" , catalog$directory )

		catalog$output_folder <- paste0( output_dir , "/" , gsub( ":|,|\\(|\\)" , "" , tolower( catalog$type ) ) , "/" )

		catalog

	}
						
		
lodown_psid <-
	function( data_name = "psid" , catalog , ... ){

		tf <- tempfile()

		if( !( 'your_email' %in% names(list(...)) ) ) stop( "`your_email` parameter must be specified.  create an account at https://ssl.isr.umich.edu/hrs/reg_pub2.php" )

		if( !( 'your_password' %in% names(list(...)) ) ) stop( "`your_password` parameter must be specified.  create an account at https://ssl.isr.umich.edu/hrs/reg_pub2.php" )
		
		your_email <- list(...)[["your_email"]]
						
		your_password <- list(...)[["your_password"]]
		
				
		# follow the authentication technique described on this stackoverflow post
		# http://stackoverflow.com/questions/15853204/how-to-login-and-then-download-a-file-from-aspx-web-pages-with-r


		# initiate and then set a curl handle to store information about this download
		curl = RCurl::getCurlHandle()

		RCurl::curlSetOpt(
			cookiejar = tempfile() , 
			followlocation = TRUE , 
			autoreferer = TRUE , 
			curl = curl
		)

		# connect to the login page to download the contents of the `viewstate` option
		html <- RCurl::getURL( 'http://simba.isr.umich.edu/u/Login.aspx' , curl = curl )

		# extract the `viewstate` string
		viewstate <- as.character( sub( '.*id="__VIEWSTATE" value="([0-9a-zA-Z+/=]*).*' , '\\1' , html ) )

		# extract the `eventvalidation` string
		eventvalidation <- as.character( sub( '.*id="__EVENTVALIDATION" value="([0-9a-zA-Z+/=]*).*' , '\\1' , html ) )

		# construct a list full of parameters to pass to the umich website
		params <- 
			list(
				'ctl00$ContentPlaceHolder1$Login1$UserName'    = your_email ,
				'ctl00$ContentPlaceHolder1$Login1$Password'    = your_password ,
				'ctl00$ContentPlaceHolder1$Login1$LoginButton' = 'Log In' ,
				'__VIEWSTATE'                                  = viewstate ,
				'__EVENTVALIDATION'                            = eventvalidation
			)
		# and now, with the username, password, and viewstate parameters all squared away
		# it's time to start downloading individual files from the umich website	

		for ( i in seq_len( nrow( catalog ) ) ){

			# logs into the umich form
			html = RCurl::postForm('http://simba.isr.umich.edu/U/Login.aspx', .params = params, curl = curl)
			
			# confirms the result's contents contains the word `Logout` because
			# if it does not contain this text, you're not logged in.  sorry.
			if ( !grepl('Logout', html ) ) stop( 'no longer logged in' )
		
			# download the file
			this_file <- cachaca( catalog[ i , "full_url" ] , FUN = RCurl::getBinaryURL , curl = curl , filesize_fun = 'unzip_verify' )

			writeBin( this_file , tf )
			
			unzipped_files <- unzip( tf , exdir = paste0( tempdir() , "/unzips" ) )


			# figure out which file contains the data (so no readmes or technical docs)
			dat_files <- unzipped_files[ grepl( ".txt" , tolower( unzipped_files ) , fixed = TRUE ) & ! grepl( "_vdm|readme|doc|errata" , tolower( unzipped_files ) ) ]
			
			# figure out which file contains the sas importation script
			sas_files <- unzipped_files[ grepl( '.sas' , unzipped_files , fixed = TRUE ) ]

			for( dat_num in seq_along( dat_files ) ){
			
				sas_name <- gsub( "\\.txt" , "" , basename( dat_files[ dat_num ] ) , ignore.case = TRUE )
				
				this_sas <- sas_files[ grep( sas_name , sas_files , ignore.case = TRUE ) ]
				
				# read the text file directly into an R data frame with `read.SAScii`
				x <- read_SAScii( dat_files[ dat_num ] , this_sas , na = c( "NA" , "." , "" ) )

				# add a `one` column
				x$one <- 1
				
				# convert all column names to lowercase
				names( x ) <- tolower( names( x ) )

				save_name <- paste0( catalog[ i , 'output_folder' ] , "/" , gsub( ":|,|\\(|\\)" , "" , tolower( catalog[ i , 'table_name' ] ) ) , if( length( dat_files ) > 1 ) tolower( paste0( " " , sas_name ) ) , ".rda" )
				
				save( x , file = save_name )
				
			}

			# delete the temporary files
			suppressWarnings( file.remove( tf , unzipped_files ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

		}

		invisible( TRUE )

	}