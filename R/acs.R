get_catalog_acs <-
	function( data_name = "acs" , output_dir , include_puerto_rico = TRUE , ... ){

		catalog <- NULL
	
		pums_ftp <- "https://www2.census.gov/programs-surveys/acs/data/pums/"
	
		ftp_listing <- rvest::html_table( xml2::read_html( pums_ftp ) )[[1]][ , "Name" ]

		suppressWarnings( available_years <- as.numeric( gsub( "/" , "" , ftp_listing ) ) )
		
		available_years <- available_years[ !is.na( available_years ) ]
		
		# remove files prior to 2005
		available_years <- available_years[ available_years >= 2005 ]
		
		for( this_year in available_years ){
		
			cat( paste0( "loading " , data_name , " catalog from " , paste0( pums_ftp , this_year ) , "\r\n\n" ) )

			if( this_year < 2007 ){
			
				available_periods <- "1-Year"
				
				available_folders <- paste0( pums_ftp , this_year )
				
			} else {
			
				ftp_listing <- rvest::html_table( xml2::read_html( paste0( pums_ftp , this_year ) ) )[[1]][ , "Name" ]
			
				available_periods <- gsub( "/$" , "" , grep( "-Year" , ftp_listing , value = TRUE ) )
			
				available_folders <- paste0( pums_ftp , this_year , "/" , available_periods )
			
			}
			
			for( i in seq_along( available_folders ) ){
			
				this_tablename <- paste0( "acs" , this_year , "_" , substr( available_periods[ i ] , 1 , 1 ) , "yr" )
			
				catalog <-
					rbind( 
						catalog ,
						data.frame(
							year = this_year ,
							time_period = available_periods[ i ] ,
							base_folder = paste0( available_folders[ i ] , "/" ) ,
							db_tablename = this_tablename ,
							dbfile = paste0( output_dir , "/SQLite.db" ) ,
							output_filename = paste0( output_dir , "/" , this_tablename , '.rds' ) ,
							include_puerto_rico = TRUE ,
							stringsAsFactors = FALSE
						)
					)
					
			}
			
		}
		
		catalog
	
	}

lodown_acs <-
	function( data_name = "acs" , catalog , ... ){

		on.exit( print( catalog ) )

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			# open the connection to the monetdblite database
			db <- DBI::dbConnect( RSQLite::SQLite() , catalog[ i , 'dbfile' ] )

			for( j in c( "h" , "p" ) ){

				# download the wyoming structure file
				wyoming_unix <- paste0( catalog[ i , 'base_folder' ] , "unix_" , j , "wy.zip" )
				
				cachaca( wyoming_unix , tf , mode = 'wb' , filesize_fun = "httr" )

				unzipped_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , "/unzips" ) )

				wyoming_table <- haven::read_sas( unzipped_files[ grep( 'sas7bdat' , unzipped_files ) ] )
				
				# identify all factor/character columns
				facchar <- tolower( names( wyoming_table )[ !( sapply( wyoming_table , class ) %in% c( 'numeric' , 'integer' ) ) ] )	
				
				# save it in `headers.h` or `headers.p`
				if ( j == 'h' ) headers.h <- facchar else headers.p <- facchar
								
				rm( wyoming_table )

				file_locations <- paste0( catalog[ i , 'base_folder' ] , "csv_" , j , c( "us.zip" , if( catalog[ i , 'include_puerto_rico' ] ) "pr.zip" ) )

				fn <- tfn <- NULL
				
				for ( this_download in file_locations ){
					
					cachaca( this_download , tf , mode = 'wb' , filesize_fun = "httr" )
								
					archive::archive_extract( tf , dir = tempdir() )

					tfn <- list.files( tempdir() , full.names = TRUE )

					# limit the files to read in to ones containing csvs
					tfn <- grep( '\\.csv$' , tfn , value = TRUE )

					# store the final csv files
					fn <- unique( c( fn , tfn ) )
				
				}
				
				# initiate the table in the database using any of the csv files #
				csvpath <- fn[ 1 ]
			
				# read in the first five hundred records of the csv file
				headers <- read.csv( csvpath , nrows = 500 )

				# figure out the column type (class) of each column
				cl <- sapply( headers , class )
				
				# convert all column names to lowercase
				names( headers ) <- tolower( names( headers ) )
				
				# if one of the column names is the word 'type'
				# change it to 'type_' -- monetdb doesn't like columns called 'type'
				if ( 'type' %in% tolower( names( headers ) ) ){
					
					cat( "note: column name 'type' unacceptable in monetdb.  changing to 'type_'\r\n\n" )
					
					names( headers )[ names( headers ) == 'type' ] <- 'type_'
					
					headers.h[ headers.h == 'type' ] <- 'type_'
				}

				# the american community survey data only contains integers and character strings..
				# so store integer columns as numbers and all others as characters
				# note: this won't work on other data sets, since they might have columns with non-integers (decimals)
				colTypes <- ifelse( cl == 'integer' , 'DOUBLE PRECISION' , 'STRING' )
				
				# create a character vector grouping each column name with each column type..
				colDecl <- paste( names( headers ) , colTypes )

				# ..and then construct an entire 'create table' sql command
				sql <-
					sprintf(
						paste(
							"CREATE TABLE" ,
							j ,
							"(%s)"
						) ,
						paste(
							colDecl ,
							collapse = ", "
						)
					)
				
				# actually execute the 'create table' sql command
				DBI::dbSendQuery( db , sql )

				# end of initiating the table in the database #
				
				# loop through each csv file
				for ( csvpath in fn ){
									
					# if the puerto rico file is out of order, read in the whole file and fix it.
					if( grepl( "pr\\.csv$" , csvpath ) ){
					
						pr_header <- tolower( names( read.csv( csvpath , nrows = 1 ) ) )
						
						pr_header[ pr_header == 'type' ] <- 'type_'
						
						if( !all( pr_header %in% names( headers ) ) ) stop( "this puerto rico file does not have the same columns" )
						
						# otherwise, maybe they're just out of order
						if( !all( names( headers ) == pr_header ) ){
						
							# read in the whole (pretty small) file
							pr_csv <- read.csv( csvpath , stringsAsFactors = FALSE )
							
							# lowercase and add an underscore to the `type` column
							names( pr_csv ) <- tolower( names( pr_csv ) )
							names( pr_csv )[ names( pr_csv ) == 'type' ] <- 'type_'
							
							# sort the `data.frame` object to match the ordering in the monetdb table
							pr_csv <- pr_csv[ DBI::dbListFields( db , j ) ]
							
							# save the `data.frame` to the disk, now that the columns are correctly ordered
							write.csv( pr_csv , csvpath , row.names = FALSE , na = '' )
							
							# remove the object and clear up ram
							rm( pr_csv ) ; gc()
							
						}
						
					}
					
					# now try to copy the current csv file into the database
					first.attempt <-
						try( {
							DBI::dbSendQuery( 
								db , 
								paste0( 
									"copy offset 2 into " , 
									j , 
									" from '" , 
									normalizePath( csvpath ) , 
									"' using delimiters ',','\\n','\"'  NULL AS ''" 
								) 
							) 
						} , silent = TRUE )
					
					# if the first.attempt did not work..
					if ( class( first.attempt ) == 'try-error' ){


						# get rid of any comma-space-comma values.
						incon <- file( csvpath , "r") 
						tf_out <- tempfile()
						outcon <- file( tf_out , "w") 
						while( length( line <- readLines( incon , 1 ) ) > 0 ){
							# remove all whitespace
							line <-  gsub( ", ," , ",," , gsub( ",( +)," , ",," , line ) )
							writeLines( line , outcon )
						}
						
						close( outcon )
						close( incon , add = TRUE )
		
						# and run the exact same command again.
						second.attempt <-
							try( {
								DBI::dbSendQuery( 
									db , 
									paste0( 
										"copy offset 2 into " , 
										j , 
										" from '" , 
										normalizePath( tf_out ) , 
										"' using delimiters ',','\\n','\"'  NULL AS ''" 
									) 
								) 
							} , silent = TRUE )
							
					} else {
					
						# if the first attempt worked,
						# the second attempt should also not be a `try-error`
						second.attempt <- NULL
						
					}
					
					# some of the acs files have multiple values that should be treated as NULL, (like acs2010_3yr_p)
					# so if the above copy-into attempts fail twice,
					# scan through the entire file and remove every instance of "N.A."
					# then re-run the copy-into line.
					
					# if the first attempt doesn't work..
					if ( class( second.attempt ) == 'try-error' ){
						
						# create a temporary output file
						fpo <- tempfile()

						# create a read-only file connection from the original file
						fpx <- file( normalizePath( tf_out ) , 'r' )
						# create a write-only file connection to the temporary file
						fpt <- file( fpo , 'w' )

						# loop through every line in the original file..
						while ( length( line <- readLines( fpx , 1 ) ) > 0 ){
						
							# replace 'N.A.' with nothings..
							line <- gsub( "N.A." , "" , line , fixed = TRUE )
							
							# and write the result to the temporary file connection
							writeLines( line , fpt )
						}
						
						# close the temporary file connection
						close( fpt )
						
						# re-run the copy into command..
						DBI::dbSendQuery( 
								db , 
								paste0( 
									"copy offset 2 into " , 
									j , 
									" from '" , 
									fpo , 						# only this time, use the temporary file as the source file
									"' using delimiters ',','\\n','\"'  NULL AS ''" 
								) 
						) 
						
						# delete the temporary files from the disk
						file.remove( fpo )
						file.remove( tf_out )
					}

					
					# erase the first.attempt object (which stored the result of the original copy-into line)
					first.attempt <- NULL
					
					
					
					# these files require lots of temporary disk space,
					# so delete them once they're part of the database
					suppressWarnings( file.remove( csvpath ) )
						
				}

			}
				
			############################################
			# create a merged (household+person) table #
			
			# figure out the fields to keep
			
			# pull all fields from the person..
			pfields <- names( DBI::dbGetQuery( db , paste0( "select * from p limit 1") ) )
			# ..and household tables
			hfields <- names( DBI::dbGetQuery( db , paste0( "select * from h limit 1") ) )
			
			# then throw fields out of the person file that match fields in the household table
			pfields <- pfields[ !( pfields %in% hfields ) ]
			# and also throw out the 'rt' field from the household table
			hfields <- hfields[ hfields != 'rt' ]
			
			# construct a massive join statement		
			i.j <-
				paste0(
					"create table " ,					# create table statement
					catalog[ i , 'db_tablename' ] , " as select " ,				# select from statement
					"'M' as rt, " ,
					paste( paste0( 'a.' , hfields ) , collapse = ", " ) ,
					", " ,
					paste( pfields , collapse = ", " ) ,
					" from h as a inner join p as b " ,
					"on a.serialno = b.serialno with data" 
				)
			
			# create the merged `headers` structure files to make the check.factors=
			# component of the sqlrepsurvey() functions below run much much faster.
			headers.m <- unique( c( headers.h , headers.p ) )
			
			# create the merged table
			DBI::dbSendQuery( db , i.j )
			
			# add columns named 'one' to each table..
			DBI::dbSendQuery( db , paste0( 'alter table ' , catalog[ i , 'db_tablename' ] , ' add column one int' ) )

			# ..and fill them all with the number 1.
			DBI::dbSendQuery( db , paste0( 'UPDATE ' , catalog[ i , 'db_tablename' ] , ' SET one = 1' ) )
			
			# confirm that the merged file has the same number of records as the person file
			stopifnot( 
				DBI::dbGetQuery( db , paste0( "select count(*) as count from p" ) ) == 
				DBI::dbGetQuery( db , paste0( "select count(*) as count from " , catalog[ i , 'db_tablename' ] ) )
			)
			
			DBI::dbRemoveTable( db , 'h' )
			DBI::dbRemoveTable( db , 'p' )

			# special exception for the 2009 3-year file..  too many missings in the weights.
			if( catalog[ i , 'year' ] <= 2009 & catalog[ i , 'time_period' ] %in% c( '3-Year' , '5-Year' ) ){
			
				# identify all weight columns
				wgt_cols <- grep( "wgt" , DBI::dbListFields( db , catalog[ i , 'db_tablename' ] ) , value = TRUE )
				
				# loop through all weight columns
				for ( this_column in wgt_cols ){
				
					# set missing values to zeroes
					DBI::dbSendQuery( db , paste( "UPDATE" , catalog[ i , 'db_tablename' ] , "SET" , this_column , "=0 WHERE" , this_column , "IS NULL" ) )
				
				}
				
			}
			
			
			# create a svrep complex sample design object
			# using the merged (household+person) table
			
			acs_design <-
				survey::svrepdesign(
					weight = ~pwgtp ,
					repweights = 'pwgtp[0-9]+' ,
					scale = 4 / 80 ,
					rscales = rep( 1 , 80 ) ,
					mse = TRUE ,
					type = 'JK1' ,
					data = catalog[ i , 'db_tablename' ]  ,
					dbtype = "SQLite" ,
					dbname = catalog[ i , 'dbfile' ]
				)
				
			# workaround for a bug in survey::svrepdesign.character
			acs_design$mse <- TRUE

			# save both complex sample survey designs
			# into a single r data file (.rds) that can now be
			# analyzed quicker than anything else.
			saveRDS( acs_design , file = catalog[ i , 'output_filename' ] )

			# add the number of records to the catalog
			catalog[ i , 'case_count' ] <- nrow( acs_design )
			
			close( acs_design )

			# disconnect from the current monet database
			DBI::dbDisconnect( db , shutdown = TRUE )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'db_tablename' ] , "'\r\n\n" ) )

		}
		
		on.exit()
				
		catalog

	}
