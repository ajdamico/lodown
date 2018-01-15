get_catalog_ipums <-
	function( data_name = "ipums" , output_dir , ... ){

		if( !( 'project' %in% names(list(...)) ) || !( list(...)[["project"]] %in% c( "usa" , "cps" , "international" ) ) ) stop( "`project` parameter must be specified.  choices are 'usa' , 'cps' , 'international'" )

		if( !( 'your_email' %in% names(list(...)) ) ) stop( "`your_email` parameter must be specified.  create an account at https://www.ipums.org/" )

		if( !( 'your_password' %in% names(list(...)) ) ) stop( "`your_password` parameter must be specified.  create an account at https://www.ipums.org/" )

		project <- list(...)[["project"]]

		your_email <- list(...)[["your_email"]]

		your_password <- list(...)[["your_password"]]

		this_cookie <- authenticate_ipums( your_email = your_email , your_password = your_password , project = project )
		
		this_download <- httr::GET( paste0( "https://" , project , ".ipums.org/" , project , "-action/extract_requests/download" ) , httr::set_cookies( .cookies = this_cookie ) )
		
		catalog <- rvest::html_table( httr::content( this_download ) )[[2]]
		
		names( catalog ) <- stringr::str_trim( paste( names( catalog ) , catalog[ 1 , ] ) )
		
		catalog <- catalog[ -1 , ]
		
		catalog[ , ] <- sapply( catalog[ , ] , function( z ) gsub( "( +)" , " " , gsub( "\n" , " " , z ) ) )

		names( catalog ) <- gsub( " (click to edit)" , "" , names( catalog ) , fixed = TRUE )
		
		catalog <- catalog[ , !( names( catalog ) %in% "Hide selections Show all" ) ]

		project_sub <- ifelse( project == 'international' , 'ipumsi' , project )
		
		catalog$full_url <- ifelse( catalog[ , "Formatted Data" ] == "CSV" , paste0( "https://" , project , ".ipums.org/" , project , "-action/downloads/extract_files/" , project_sub , "_" , stringr::str_pad( catalog[ , "Extract Number" ] , 5 , pad = '0' ) , ".csv.gz" ) , NA )

		catalog$xml_url <- gsub( "\\.csv\\.gz" , ".xml" , catalog$full_url )
		
		catalog$db_tablename <- ifelse( is.na( catalog$full_url ) , NA , gsub( "( +)" , "_" , stringr::str_trim( gsub( "[^a-z0-9]" , " " , tolower( catalog$Description ) ) ) ) )
		
		catalog$output_filename <- ifelse( is.na( catalog$full_url ) , NA , paste0( output_dir , '/' , catalog$db_tablename , '.rds' ) )
		
		catalog$dbfile <- ifelse( is.na( catalog$full_url ) , NA , paste0( output_dir , "/SQLite.db" ) )

		httr::GET( paste0( "https://" , project , ".ipums.org/" , project , "-action/users/logout" ) , httr::set_cookies( .cookies = this_cookie ) )
		
		catalog

	}


lodown_ipums <-
	function( data_name = "ipums" , catalog , ... ){

		on.exit( print( catalog ) )

		if( !( 'project' %in% names(list(...)) ) || !( list(...)[["project"]] %in% c( "usa" , "cps" , "international" ) ) ) stop( "`project` parameter must be specified.  choices are 'usa' , 'cps' , 'international'" )

		if( !( 'your_email' %in% names(list(...)) ) ) stop( "`your_email` parameter must be specified.  create an account at https://www.ipums.org/" )

		if( !( 'your_password' %in% names(list(...)) ) ) stop( "`your_password` parameter must be specified.  create an account at https://www.ipums.org/" )

		project <- list(...)[["project"]]

		your_email <- list(...)[["your_email"]]

		your_password <- list(...)[["your_password"]]

		this_cookie <- authenticate_ipums( your_email = your_email , your_password = your_password , project = project )

		tf <- tempfile() ; tf2 <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			if( !grepl( 'CSV' , catalog[ i , 'Formatted Data' ] ) ){
			
				cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " skipped because no csv file available.\n\nif you intended to import this extract, please visit\n`https://" , project , ".ipums.org/" , project , "-action/extract_requests/download` and revise or resubmit with the csv option checked.\r\n\n" ) )
				
			} else {

				csv_filename <- gsub( "\\.rds" , ".csv" , catalog[ i , 'output_filename' ] )

				# download the actual file
				httr::GET( catalog[ i , 'full_url' ] , httr::write_disk( tf , overwrite = TRUE ) , httr::set_cookies( .cookies = this_cookie ) , httr::progress() )

				# store the file to the local disk
				R.utils::gunzip( tf , csv_filename , overwrite = TRUE )
				
				xml <- httr::GET( catalog[ i , 'xml_url' ] , httr::set_cookies( .cookies = this_cookie ) )

				csv_file_structure <- unlist( XML::xpathSApply( XML::xmlParse( xml ) , "//*//*//*//*" , XML::xmlGetAttr , "type" ) )
		
				csv_file_structure <- csv_file_structure[ csv_file_structure != 'rectangular' ]

				# simple check that the stored csv file matches the loaded structure
				if( !( length( csv_file_structure ) == ncol( read.csv( csv_filename , nrow = 10 ) ) ) ) stop( "number of columns in final csv file does not match ipums structure xml file" )

				# decide whether column types should be character or numeric
				colTypes <- ifelse( csv_file_structure == 'character' , 'CLOB' , 'DOUBLE PRECISION' )

				# determine the column names from the csv file
				cn <- toupper( names( read.csv( csv_filename , nrow = 1 ) ) )

				# force all column names to be lowercase, since MonetDB.R is now case-sensitive
				cn <- tolower( cn )
				
				if( !is.na( catalog[ i , 'output_filename' ] ) ){

					# read in as a data.frame
					x <- 
						data.frame( 
							readr::read_csv( 
								csv_filename , 
								col_names = cn , 
								col_types = paste0( ifelse( csv_file_structure == 'character' , 'c' , 'd' ) , collapse = "" ) , 
								skip = 1 ,
								locale = readr::locale( decimal_mark = "." , grouping_mark = "," ) 
							) 
						)
					
					saveRDS( x , file = catalog[ i , 'output_filename' ] , compress = FALSE )
				
					catalog[ i , 'case_count' ] <- nrow( x )
				
					cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )
				
				}
				
				if( !is.na( catalog[ i , 'dbfile' ] ) ){
					
					# open the connection to the monetdblite database
					db <- DBI::dbConnect( RSQLite::SQLite() , catalog[ i , 'dbfile' ] )
							
					# paste column names and column types together sequentially
					colDecl <- paste( cn , colTypes )

					# construct a character string containing the create table command
					sql_create_table <- 
						sprintf( 
							paste( "CREATE TABLE" , catalog[ i , 'db_tablename' ] , "(%s)" ) ,
							paste( colDecl , collapse = ", " ) 
						)

					# construct the table in the database
					DBI::dbSendQuery( db , sql_create_table )


					# import the csv file into the database.
					DBI::dbSendQuery( 
						db , 
						paste0(
							"COPY OFFSET 2 INTO " ,
							catalog[ i , 'db_tablename' ] ,
							" FROM '" ,
							normalizePath( csv_filename ) ,
							"' USING DELIMITERS ',','\\n','\"' NULL AS ''" 
							# , " BEST EFFORT"	# <-- if your import breaks for some reason,
												# you could try uncommenting the preceding line
						)
					)


					# count the number of lines in the csv file on your local disk
					csv_lines <- R.utils::countLines( csv_filename )

					# count the number of records in the imported table
					dbtable_lines <- DBI::dbGetQuery( db , paste( 'SELECT COUNT(*) FROM' , catalog[ i , 'db_tablename' ] ) )[ 1 , 1 ]

					# the imported table should have one fewer line than the csv file,
					# because the csv file has headers
					stopifnot( csv_lines == dbtable_lines + 1 )

					catalog[ i , 'case_count' ] <- dbtable_lines
					
					cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'dbfile' ] , "'\r\n\n" ) )
					
				}
				
				# delete the temporary files
				suppressWarnings( file.remove( tf ) )
				
			}
			
		}
		
		httr::GET( paste0( "https://" , project , ".ipums.org/" , project , "-action/users/logout" ) , httr::set_cookies( .cookies = this_cookie ) )

		on.exit()
		
		catalog

	}


	
	

# thanks to the amazing respondents on stackoverflow for this algorithm
# http://stackoverflow.com/questions/34829920/how-to-authenticate-a-shibboleth-multi-hostname-website-with-httr-in-r

authenticate_ipums <-
	function( your_email , your_password , project ){
	
		tf <- tempfile()
		this_page <- httr::GET( paste0( "https://" , project , ".ipums.org/" , project , "-action/menu" ) )
		writeBin( this_page$content , tf )
		if( any( grepl( "Logout" , readLines( tf ) ) ) ) return( invisible( TRUE ) )
		
	
		httr::set_config( httr::config( ssl_verifypeer = 0L ) )

		# get first page
		# p1 <- httr::GET( paste0( "https://" , project , ".ipums.org/" , project , "-action/users/login" ) , httr::verbose( info = TRUE ) )
		p1 <- httr::GET( paste0( "https://" , project , ".ipums.org/" , project , "-action/users/login" ) )

		# post login credentials
		b2 <- list( "j_username" = your_email , "j_password" = your_password )
		
		c2 <- 
			c(
				JSESSIONID = p1$cookies[ p1$cookies$domain=="#HttpOnly_live.identity.popdata.org" , ]$value ,
				`_idp_authn_lc_key` = p1$cookies[ p1$cookies$domain == "live.identity.popdata.org" , ]$value 
			)

		p2 <- httr::POST( p1$url , body = b2 , httr::set_cookies( .cookies = c2 ) , encode = "form" )

		# parse hidden fields
		h2 <- xml2::read_html( p2$content )
		form <- rvest::html_form(h2) 

		# post hidden fields
		b3 <- 
			list( 
				"RelayState" = form[[1]]$fields[[1]]$value , 
				"SAMLResponse" = form[[1]]$fields[[2]]$value
			)
			
		c3 <- 
			c(
				JSESSIONID = p1$cookies[ p1$cookies$domain == "#HttpOnly_live.identity.popdata.org" , ]$value ,
				`_idp_session` = p2$cookies[ p2$cookies$name == "_idp_session" , ]$value ,
				`_idp_authn_lc_key` = p2$cookies[p2$cookies$name == "_idp_authn_lc_key" , ]$value 
			)
		
		p3 <- httr::POST( form[[1]]$url , body = b3 , httr::set_cookies( .cookies = c3 ) , encode = "form" )

		# get interesting page
		c4 <- 
			c(
				JSESSIONID = p3$cookies[p1$cookies$domain==paste0( project , ".ipums.org" ) && p3$cookies$name == "JSESSIONID" , ]$value ,
				`_idp_session` = p3$cookies[ p3$cookies$name == "_idp_session" , ]$value ,
				`_idp_authn_lc_key` = p3$cookies[ p3$cookies$name == "_idp_authn_lc_key" , ]$value 
			)
		
		p4 <- httr::GET( paste0( "https://" , project , ".ipums.org/" , project , "-action/menu" ) , httr::set_cookies( .cookies = c4 ) )

		# return the appropriate cookies
		c4
	}

