get_catalog_nibrs <-
	function( data_name = "nibrs" , output_dir , ... ){

		catalog <- get_catalog_icpsr( "00128" )
		
		catalog$unzip_folder <- paste0( output_dir , "/" , gsub( "[^0-9A-z ]" , "" , catalog$name ) , "/" , catalog$dataset_name )

		catalog$db_tablename <- tolower( gsub( " " , "_" , paste0( gsub( "[^0-9A-z ]" , "" , catalog$name ) , "_" , catalog$dataset_name ) ) )
		catalog$db_tablename <- gsub( "national_incidentbased_reporting_system_" , "x" , catalog$db_tablename )
		catalog$db_tablename <- gsub( "uniform_crime_reporting_" , "ucr_" , catalog$db_tablename )
		catalog$db_tablename <- gsub( "program_data_" , "" , catalog$db_tablename )
		catalog$db_tablename <- gsub( "-level_file" , "_level" , catalog$db_tablename )
		catalog$db_tablename <- gsub( "segment" , "seg" , catalog$db_tablename )
		
		catalog$unzip_folder <- gsub( "IncidentBased" , "Incident Based" , catalog$unzip_folder )

		catalog$dbfile <- paste0( output_dir , "/SQLite.db" )

		catalog

	}


lodown_nibrs <-
	function( data_name = "nibrs" , catalog , ... ){

		on.exit( print( catalog ) )

		lodown_icpsr( data_name = data_name , catalog , ... )

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			unzipped_files <- list.files( catalog[ i , 'unzip_folder' ] , full.names = TRUE )

			# open the connection to the monetdblite database
			db <- DBI::dbConnect( RSQLite::SQLite() , catalog[ i , 'dbfile' ] )

			# determine the filenames that end with `sas`
			sas.import <- unzipped_files[ grep( "sas$" , tolower( unzipped_files ) ) ]
		
			# the 2004 - 2008 files have one character field in the sas file
			# that's been designated as numeric incorrectly.  fix it.
			if( catalog[ i , 'name' ] %in% paste0( "National Incident-Based Reporting System, " , as.character( 2004:2008 ) ) ){
			
				# read in the file
				sip <- readLines( sas.import )
			
				# add a character string identifier to one field
				sip <- gsub( "V1012 46-49" , "V1012 $ 46-49" , sip )
				
				# overwrite the sas import script on the local disk
				writeLines( sip , sas.import )
			
			}
		
			# determine the filenames containing the word `data`
			data.file <- unzipped_files[ grep( "data" , tolower( basename( unzipped_files ) ) ) ]
		
			# if the current data.file is also gzipped..
			if ( grepl( "gz$" , tolower( data.file ) ) ){
			
				# gunzip it and overwrite itself in the current directory
				data.file <- R.utils::gunzip( data.file )
				
			}
			
			DBI::dbBegin(db)

			# in most cases, the sas importation script should start right at the beginning..
			beginline <- 1
			
			# read the data file into an r monetdb database
			read_SAScii_monetdb(
				fn = data.file ,
				sas_ri = sas.import ,
				tl = TRUE ,	# convert all column names to lowercase?
				tablename = catalog[ i , 'db_tablename' ] ,
				beginline = beginline ,
				skip_decimal_division = TRUE ,
				connection = db
			)
				
				
			# figure out which variables need to be recoded to system missing #
			
			# read the entire sas import script into a character vector
			recode.lines <- toupper( readLines( sas.import ) )
			
			# look for the start of the system missing recode block
			mvr <- intersect( grep( "RECODE TO SAS SYSMIS" , recode.lines ) , grep( "USER-DEFINED MISSING VALUE" , recode.lines ) )
			
			# fail if there are more than one.
			if ( length( mvr ) > 1 ) stop( "sas script has more than one sysmis recode block?" )
			
			# if there's just one..
			if ( length( mvr ) == 1 ){
				
				# isolate the recode lines
				recode.lines <- recode.lines[ mvr:length( recode.lines ) ]
				
				# find all lines that start with an IF statement and end with a semicolon
				lines.with.if <- grep( "IF (.*);" , recode.lines )
				
				# confirm all of those lines have a sas missing value (a dot) or a '' somewhere in there.
				lines.with.dots <- grep( "\\.|''" , recode.lines )

				# if the lines don't match up, fail cuz something's wrong.  terribly terribly wrong.
				if ( length( lines.with.if[ !( lines.with.if %in% lines.with.dots ) ] ) > 0 ) stop( "some recode line is recoding to something other than missing" )
				
				# further limit the recode lines to only lines containing an if block
				recodes <- recode.lines[ lines.with.if ]
				
				# break the recode lines up by semicolons, in case there's more than one per line
				recodes <- unlist( strsplit( recodes , ";" ) )
				
				# remove the word `IF `
				recodes <- gsub( "IF " , "" , recodes )
				
				# remove leading and trailing whitespace
				recodes <- stringr::str_trim( recodes )
				
				# remove empty strings
				recodes <- recodes[ recodes != '' ]
				
				# find which variables need to be replaced by extracting whatever's directly in front of the equals sign
				vtr <- stringr::str_trim( tolower( gsub( "(.*) THEN( ?)(.*)( ?)=(.*)" , "\\3" , recodes ) ) )
				
				# remove everything after the `THEN` block..
				ptm <- gsub( " THEN( ?)(.*)" , "" , recodes )
				
				# ..to create a vector of patterns to match
				ptm <- tolower( stringr::str_trim( ptm ) )
				
				# if the table has less than 100,000 records, read it into a data.frame as well
				# note: if you have lots of RAM, you might want to read it in regardless.
				# this loop assumes you have less than 4GB of RAM, so tables with more
				# than 100,000 records will not automatically get read in unless you comment
				# out this `if` block by adding `#` in front of this line and the accompanying `}`
				if ( DBI::dbGetQuery( db , paste( 'select count(*) from ' , catalog[ i , 'db_tablename' ] ) )[ 1 , 1 ] < 100000 ){
					
					# pull the data file into working memory
					x <- DBI::dbReadTable( db , catalog[ i , 'db_tablename' ] )
				
					# if there are any missing values to recode
					if ( length( mvr ) == 1 ){
					
						# loop through each variable to recode
						for ( k in seq_along( vtr ) ){
					
							# overwrite sas syntax with r syntax in the patterns to match commands.
							r.command <- gsub( "=" , "==" , ptm[ k ] )
							r.command <- gsub( " or " , "|" , r.command )
							r.command <- gsub( " and " , "&" , r.command )
							r.command <- gsub( " in \\(" , " %in% c\\(" , r.command )
							
							# wherever the pattern has been matched, overwrite the current variable with a missing
							x[ with( x , which( eval( parse( text = r.command ) ) ) ) , vtr[ k ] ] <- NA
							
							# if a column is *only* NAs then delete it
							if( all( is.na( x[ , vtr[ k ] ] ) ) ) x[ , vtr[ k ] ] <- NULL
							
							# clear up RAM
							gc()
							
						}
						
						# remove the current data table from the database
						DBI::dbRemoveTable( db , catalog[ i , 'db_tablename' ] )
						
						names( x ) <- tolower( names( x ) )
						
						# ..and overwrite it with the data.frame object
						# that you've just blessedly cleaned up
						DBI::dbWriteTable( db , catalog[ i , 'db_tablename' ] , x )

					}
					
					# save the r data.frame object to the local disk as an `.rds`
					saveRDS( x , file = gsub( "txt$" , "rds" , data.file ) , compress = FALSE )
				
					# remove the object from working memory
					rm( x )
					
					
				# if the current data table has more than 100,000 records..
				} else {
									
					# if there are any variables that need system missing-ing
					if( length( mvr ) == 1 ){
						DBI::dbSendQuery( 
							db , 
							paste(
								"UPDATE" ,
								catalog[ i , 'db_tablename' ] ,
								"SET" ,
								vtr ,
								" = NULL WHERE" ,
								ptm, ";", collapse=" "
							)
						)
					}
					
				}
				
			# otherwise, if there's no recoding to be done at all
			} else {
				
				# check whether the current table has less than 100000 records..
				if ( DBI::dbGetQuery( db , paste( 'select count(*) from ' , catalog[ i , 'db_tablename' ] ) )[ 1 , 1 ] < 100000 ){
				
					# pull the data file into working memory
					x <- DBI::dbReadTable( db , catalog[ i , 'db_tablename' ] )
				
					# save the r data.frame object to the local disk as an `.rds`
					saveRDS( x , file = gsub( "txt$" , "rds" , data.file ) , compress = FALSE )
				
				}
				
			}

			DBI::dbCommit(db)

			catalog[ i , 'case_count' ] <- DBI::dbGetQuery( db , paste0( "SELECT COUNT(*) FROM " , catalog[ i , 'db_tablename' ] ) )[ 1 , 1 ]
			
			# delete the temporary files
			file.remove( tf , unzipped_files )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'db_tablename' ] , "'\r\n\n" ) )

		}

		on.exit()
		
		catalog

	}

