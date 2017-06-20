get_catalog_brfss <-
	function( data_name = "brfss" , output_dir , ... ){
			
		data_page <- readLines( "https://www.cdc.gov/brfss/annual_data/annual_data.htm" )

		available_years <- sort( unique( gsub( "(.*)/brfss/annual_data/annual_([0-9][0-9][0-9][0-9]).htm(.*)" , "\\2" , grep( "annual_data/annual_([0-9][0-9][0-9][0-9]).htm" , data_page , value = TRUE ) ) ) )

		path_to_files <-
			ifelse( available_years < 1990 , 
				paste0( "ftp://ftp.cdc.gov/pub/data/Brfss/CDBRFS" , substr( available_years , 3 , 4 ) , "_XPT.zip" ) ,
			ifelse( available_years < 2002 , 
				paste0( "ftp://ftp.cdc.gov/pub/data/Brfss/CDBRFS" , substr( available_years , 3 , 4 ) , "XPT.zip" ) ,
			ifelse( available_years >= 2012 ,
				paste0( "https://www.cdc.gov/brfss/annual_data/" , available_years , "/files/LLCP" , available_years , "ASC.ZIP" ) ,
			ifelse( available_years == 2011 ,
				"ftp://ftp.cdc.gov/pub/data/brfss/LLCP2011ASC.ZIP" ,
				paste0( "ftp://ftp.cdc.gov/pub/data/brfss/cdbrfs" , ifelse( available_years == 2002 , available_years , substr( available_years , 3 , 4 ) ) , "asc.zip" )
				) ) ) )

		sas_files <-
			ifelse( available_years < 2002 ,
				NA ,
			ifelse( available_years >= 2012 ,
				paste0( "https://www.cdc.gov/brfss/annual_data/" , available_years , "/files/sasout" , substr( available_years , 3 , 4 ) , "_llcp.sas" ) ,
			ifelse( available_years == 2011 ,
				"https://www.cdc.gov/brfss/annual_data/2011/sasout11_llcp.sas" ,
				paste0( "https://www.cdc.gov/brfss/annual_data/" , available_years , "/files/sasout" , substr( available_years , 3 , 4 ) , ifelse( available_years > 2006 , ".SAS" , ".sas" ) ) 
				) ) )


		catalog <-
			data.frame(
				year = available_years ,
				db_tablename = paste0( 'x' , available_years ) ,
				full_url = path_to_files ,
				sas_ri = sas_files ,
				dbfolder = paste0( output_dir , "/MonetDB" ) ,
				weight = c( rep( 'x_finalwt' , 18 ) , rep( 'xfinalwt' , 9 ) , rep( 'xllcpwt' , length( available_years ) - 27 ) ) ,
				psu = c( rep( 'x_psu' , 18 ) , rep( 'xpsu' , length( available_years ) - 18 ) ) ,
				strata = c( rep( 'x_ststr' , 18 ) , rep( 'xststr' , length( available_years ) - 18 ) ) ,
				design_filename = paste0( output_dir , "/" , available_years , " design.rds" ) ,
				stringsAsFactors = FALSE
			)

		catalog

	}


lodown_brfss <-
	function( data_name = "brfss" , catalog , ... ){

		tf <- tempfile() ; impfile <- tempfile() ; sasfile <- tempfile() ; csvfile <- tempfile()

		
		for ( i in seq_len( nrow( catalog ) ) ){

			# open the connection to the monetdblite database
			db <- DBI::dbConnect( MonetDBLite::MonetDBLite() , catalog[ i , 'dbfolder' ] )

			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

			unzipped_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , "/unzips" ) )

			if( is.na( catalog[ i , 'sas_ri' ] ) ){
			
				# read the sas transport file into r
				x <- foreign::read.xport( unzipped_files ) 
				
				# convert all column names in the table to all lowercase
				names( x ) <- tolower( names( x ) )
				
				# do not allow this illegal sql column name
				names( x )[ names( x ) == 'level' ] <- 'level_'
				
				# immediately export the data table to a comma separated value (.csv) file,
				# also stored on the local hard drive
				write.csv( x , csvfile , row.names = FALSE )

				# count the total number of records in the table
				# rows to check then read
				rtctr <- nrow( x )
				
				# prepare to handle errors if they occur (and they do occur)
				# reset all try-error objects
				first.attempt <- second.attempt <- NULL

				# first try to read the csv file into the monet database with NAs for NA strings
				first.attempt <- try( DBI::dbWriteTable( db , catalog[ i , 'db_tablename' ] , csvfile , na.strings = "NA" , nrow.check = rtctr , lower.case.names = TRUE ) , silent = TRUE )
				
				# if the dbWriteTable() function returns an error instead of working properly..
				if( class( first.attempt ) == "try-error" ) {
				
					# try re-exporting the csv file (overwriting the original csv file)
					# using "" for the NA strings
					write.csv( x , csvfile , row.names = FALSE , na = "" )
					
					# try to remove the data table from the monet database
					try( DBI::dbRemoveTable( db , catalog[ i , 'db_tablename' ] ) , silent = TRUE )
					
					# and re-try reading the csv file directly into the monet database, this time with a different NA string setting
					second.attempt <-
						try( DBI::dbWriteTable( db , catalog[ i , 'db_tablename' ] , csvfile , na.strings = "" , nrow.check = rtctr , lower.case.names = TRUE ) , silent = TRUE )
				}

				# if that still doesn't work, import the table manually
				if( class( second.attempt ) == "try-error" ) {
				
					# try to remove the data table from the monet database
					try( DBI::dbRemoveTable( db , catalog[ i , 'db_tablename' ] ) , silent = TRUE )
				
					# determine the class of each element of the brfss data table (it's either numeric or its not)
					colTypes <- 
						ifelse( 
							sapply( x , class ) == 'numeric' , 
							'DOUBLE PRECISION' , 
							'VARCHAR(255)' 
						)
					
					# combine the column names with their respective types,
					# into a single character vector containing every field
					colDecl <- paste( names( x ) , colTypes )

					# build the full sql CREATE TABLE string that will be used
					# to create the data table in the monet database
					sql.create <-
						sprintf(
							paste(
								"CREATE TABLE" ,
								catalog[ i , 'db_tablename' ] ,
								"(%s)"
							) ,
							paste(
								colDecl ,
								collapse = ", "
							)
						)
					
					# create the table in the database
					DBI::dbSendQuery( db , sql.create )
					
					# now build the sql command that will copy all records from the csv file (still on the local hard disk)
					# into the monet database, using the structure that's just been defined by the sql.create object above
					sql.update <- 
						paste0( 
							"copy " , 
							rtctr , 
							" offset 2 records into " , 
							catalog[ i , 'db_tablename' ] , 
							" from '" , 
							csvfile , 
							"' using delimiters ',' null as ''" 
						)
						
					# run the sql command
					DBI::dbSendQuery( db , sql.update )
						
				}
			
			} else {
			
				sas_con <- file( catalog[ i , 'sas_ri' ] , "r" , encoding = "windows-1252" )
				z <- readLines( sas_con )
				close( sas_con )
						
				# throw out a few columns that cause importation trouble with monetdb
				if ( catalog[ i , 'year' ] == 2009 ) z <- z[ -159:-168 ]
				if ( catalog[ i , 'year' ] == 2011 )	z <- z[ !grepl( "CHILDAGE" , z ) ]
				if ( catalog[ i , 'year' ] == 2013 ) z[ 361:362 ] <- c( "_FRTLT1z       2259" , "_VEGLT1z       2260" )
				if ( catalog[ i , 'year' ] == 2014 ) z[ 86 ] <- "COLGHOUS $ 64"

				if( catalog[ i , 'year' ] == 2015 ){
				
					z <- gsub( "\\\f" , "" , z )
					z <- gsub( "_FRTLT1       2056" , "_FRTLT1_       2056" , z )
					z <- gsub( "_VEGLT1       2057" , "_VEGLT1_       2057" , z )
					
				}
				
				# replace all underscores in variable names with x's
				z <- gsub( "_" , "x" , z , fixed = TRUE )
				
				# throw out these three fields, which overlap other fields and therefore are not supported by SAScii
				# (see the details section at the bottom of page 9 of http://cran.r-project.org/web/packages/SAScii/SAScii.pdf for more detail)
				z <- z[ !grepl( "SEQNO" , z ) ]
				z <- z[ !grepl( "IDATE" , z ) ]
				z <- z[ !grepl( "PHONENUM" , z ) ]
				
				# remove all special characters
				z <- gsub( "\t" , " " , z , fixed = TRUE )
				z <- gsub( "\f" , " " , z , fixed = TRUE )
				
				# re-write the sas importation script to a file on the local hard drive
				writeLines( z , impfile )

				# if it's 2013 or beyond..
				if ( catalog[ i , 'year' ] >= 2013 ){
					
					# create a read connection..
					incon <- file( unzipped_files , "r" , encoding = "windows-1252" )
					
					# ..and a write connection
					outcon <- file( sasfile , "w" )
				
					# read through every line
					while( length( line <- readLines( incon , 1 , skipNul = TRUE ) ) > 0 ){
					
						# remove the stray slash
						line <- gsub( "\\" , " " , line , fixed = TRUE )
						
						# remove the stray everythings
						line <- gsub( "[^[:alnum:]///' \\.]" , " " , line )
						
						# mac/unix converts some weird characters to two digits
						# while windows convers the to one.  deal with it.
						line <- iconv( line , "" , "ASCII" , sub = "abcxyz" )
						line <- gsub( "abcxyzabcxyz" , " " , line )
						line <- gsub( "abcxyz" , " " , line )
				
						# write the result to the output connection
						writeLines( line , outcon )
						
					}
					
					# remove the original
					file.remove( unzipped_files )
					
					# redirect the local filename to the new file
					unzipped_files <- sasfile
					
					# close both connections
					close( outcon )
					close( incon )
					
				}
				
				# actually run the read.SAScii.monetdb() function
				# and import the current fixed-width file into the monet database
				read_SAScii_monetdb (
					unzipped_files ,
					impfile ,
					beginline = 70 ,
					zipped = F ,						# the ascii file is no longer stored in a zipped file
					tl = TRUE ,							# convert all column names to lowercase
					tablename = catalog[ i , 'db_tablename' ] ,	# the table will be stored in the monet database as bYYYY.. for example, 2010 will be stored as the 'b2010' table
					connection = db
				)
				
			}
			
			# add a column containing all ones to the current table
			DBI::dbSendQuery( db , paste0( 'alter table ' , catalog[ i , 'db_tablename' ] , ' add column one int' ) )
			DBI::dbSendQuery( db , paste0( 'UPDATE ' , catalog[ i , 'db_tablename' ] , ' SET one = 1' ) )
			
			# create a database-backed complex sample design object
			brfss_design <-
				survey::svydesign(
					weight = as.formula( paste( "~" , catalog[ i , 'weight' ] ) ) ,
					nest = TRUE ,
					strata = as.formula( paste( "~" , catalog[ i , 'strata' ] ) ) ,
					id = as.formula( paste( "~" , catalog[ i , 'psu' ] ) ) ,
					data = catalog[ i , 'db_tablename' ] ,
					dbtype = "MonetDBLite" ,
					dbname = catalog[ i , 'dbfolder' ]
				)

			# save the complex sample survey design
			# into a single r data file (.rds) that can now be
			# analyzed quicker than anything else.
			saveRDS( brfss_design , file = catalog[ i , 'design_filename' ] )

			# add the number of records to the catalog
			catalog[ i , 'case_count' ] <- nrow( brfss_design )

			# repeat.
			
			# disconnect from the current monet database
			DBI::dbDisconnect( db , shutdown = TRUE )

			# delete the temporary files
			suppressWarnings( file.remove( tf , impfile , unzipped_files , sasfile , csvfile ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'db_tablename' ] , "'\r\n\n" ) )

		}

		catalog

	}

