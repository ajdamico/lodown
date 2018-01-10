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

				wyoming_table <- data.frame( haven::read_sas( unzipped_files[ grep( 'sas7bdat' , unzipped_files ) ] ) )
				
				names( wyoming_table ) <- tolower( names( wyoming_table ) )
				
				headers <- names( wyoming_table )
				
				if( j == 'h' ) headers.h <- headers else headers.p <- headers
				
				cc <- sapply( wyoming_table , class )

				DBI::dbWriteTable( db , j , wyoming_table[ FALSE , , drop = FALSE ] , overwrite = TRUE , append = FALSE )
				
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
				
				for( this_csv in fn ){
					
					# initiate the current table
					DBI::dbWriteTable( 
						db , 
						j , 
						this_csv , 
						overwrite = FALSE , 
						append = TRUE ,
						header = FALSE ,
						skip = 1 ,
						colClasses = cc
					)
				
				}
				
				# these files require lots of temporary disk space,
				# so delete them once they're part of the database
				suppressWarnings( file.remove( fn ) )
				
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
					"on a.serialno = b.serialno" 
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
