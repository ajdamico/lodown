get_catalog_nhts <-
	function( data_name = "nhts" , output_dir , ... ){

		nhts_datapage <- "http://nhts.ornl.gov/download.shtml"
	
		link_refs <- rvest::html_attr( rvest::html_nodes( xml2::read_html( nhts_datapage ) , "a" ) , "href" )
			
		link_text <- rvest::html_text( rvest::html_nodes( xml2::read_html( nhts_datapage ) , "a" ) )
	
		stopifnot( length( link_refs ) == length( link_text ) )

		dl_text <- link_text[ grep( "download/" , link_refs ) ]
	
		dl_refs <- grep( "download/" , link_refs , value = TRUE )

		csv_text <- dl_text[ grep( "ascii|tripchain|roster\\.zip" , dl_refs , ignore.case = TRUE ) ]
		
		csv_refs <- dl_refs[ grep( "ascii|tripchain|roster\\.zip" , dl_refs , ignore.case = TRUE ) ]
			
		catalog <-
			data.frame(
				year = substr( csv_refs , 1 , 4 ) ,
				full_url = paste0( "http://nhts.ornl.gov/" , csv_refs ) ,
				file_info = csv_text ,
				dbfile = paste0( output_dir , "/SQLite.db" ) ,
				stringsAsFactors = FALSE
			)

		catalog$output_folder <- paste0( output_dir , "/" , catalog$year , "/" )

		catalog <- catalog[ !( catalog$year >= 2009 & grepl( "tripchaining\\.zip" , basename( catalog$full_url ) , ignore.case = TRUE ) ) , ]
			
		catalog

	}


lodown_nhts <-
	function( data_name = "nhts" , catalog , ... ){

		on.exit( print( catalog ) )

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			# open the connection to the monetdblite database
			db <- DBI::dbConnect( RSQLite::SQLite() , catalog[ i , 'dbfile' ] )

			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

			unzipped_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , "/unzips" ) )

			
			# read in csv and asc files
			for( this_csv in grep( "\\.csv$|\\.asc$" , unzipped_files , value = TRUE , ignore.case = TRUE ) ){
			
				db_tablename <- paste0( tolower( gsub( "[0-9]+" , "" , gsub( "(.*)\\.(.*)" , "\\1" , basename( this_csv ) ) ) ) , catalog[ i , 'year' ] )
				
				if( catalog[ i , 'year' ] == 1983 ){
				
					x <- read.csv( this_csv , stringsAsFactors = FALSE )
					
					names( x ) <- tolower( names( x ) )
					
					DBI::dbWriteTable( db , db_tablename , x , header = TRUE , row.names = NULL , nrow.check = 250000 , lower.case.names = TRUE , newline = '\\r\\n' )
						
				} else {
					
					DBI::dbWriteTable( db , db_tablename , this_csv , header = TRUE , row.names = NULL , nrow.check = 250000 , lower.case.names = TRUE , newline = '\\r\\n' )
					
				}
			
			}
			
			# read in sas7bdat files
			for( this_sas in grep( "\\.sas7bdat$" , unzipped_files , value = TRUE , ignore.case = TRUE ) ){
		
				db_tablename <- paste0( tolower( gsub( "[0-9]+" , "" , gsub( "(.*)\\.(.*)" , "\\1" , basename( this_sas ) ) ) ) , catalog[ i , 'year' ] )
				
				x <- data.frame( haven::read_sas( this_sas ) )

				names( x ) <- tolower( names( x ) )
				
				DBI::dbWriteTable( db , db_tablename , x , header = TRUE , row.names = FALSE )

			}
			
			# read in sas transport files
			for( this_xpt in grep( "\\.xpt$" , unzipped_files , value = TRUE , ignore.case = TRUE ) ){
		
				db_tablename <- paste0( tolower( gsub( "[0-9]+" , "" , gsub( "(.*)\\.(.*)" , "\\1" , basename( this_xpt ) ) ) ) , catalog[ i , 'year' ] )
				
				x <- foreign::read.xport( this_xpt )

				names( x ) <- tolower( names( x ) )
				
				DBI::dbWriteTable( db , db_tablename , x , header = TRUE , row.names = FALSE )

			}
			
			# read in ascii + .sas files
			for( this_txt in grep( "\\.txt$" , unzipped_files , value = TRUE , ignore.case = TRUE ) ){
		
				# find the .lst filepath with the same name as the .txt
				lst.filepath <- gsub( 'txt$|TXT$' , 'lst' , this_txt )
			
				# the 1995 person .lst file isn't cased the same as its data file.
				lst.filepath <- gsub( "PERS95_2" , "pers95_2" , lst.filepath )
			
				# read in the lsc file
				lst.file <- tolower( readLines( lst.filepath ) )
				
				# identify the `field` row
				first.field <- min( grep( 'field' , lst.file ) )

				lf <- readLines( lst.filepath )
				lf <- lf[ ( first.field + 1 ):length( lf ) ]
				while( any( grepl( "   " , lf ) ) ) lf <- gsub( "   " , "  " , lf )
				lf <- gsub( "^  " , "" , lf )
				lf <- lf[ lf != '' ]
				lf <- gsub( "([0-9])  ([0-9])" , "\\1.\\2" , lf )
				lf <- gsub( "  " , "," , lf )
				lf.tf <- tempfile()
				writeLines( lf , lf.tf )
				stru <- read.csv( lf.tf , header = FALSE )
				
				stru[ , 1 ] <- as.character( stru[ , 1 ] )
			
				# remove any blank fields at the end
				stru <- stru[ !is.na( as.numeric( stru[ , 1 ] ) ) , ]
			
				# extract the field structure
				txt.field <- tolower( stringr::str_trim( stru[ , 2 ] ) )
				txt.type <- stringr::str_trim( stru[ , 3 ] )
				txt.w <- stringr::str_trim( stru[ , 4 ] )

				# pull only the characters before the extension					
				fn.before.dot <- gsub( "\\.(.*)" ,"" , basename( this_txt ) )

				# make the tablename the first three letters of the filename,
				# remove any numbers, also any underscores
				tablename <- tolower( paste0( gsub( "_" , "" , gsub( "[0-9]+" , "" , fn.before.dot ) , fixed = TRUE ) ,  catalog[ i , 'year' ] ) )
				
				# import the actual text file into working memory
				x <- 
					readr::read_fwf( 
						this_txt , 
						col_positions = readr::fwf_widths( floor( as.numeric( txt.w ) ) , col_names = txt.field ) ,
						na = c( 'NA' , '' , ' ' ) ,
						col_types = paste( ifelse( txt.type == 'Numeric' , 'd' , 'c' ) , collapse = "" ) ,
						locale = readr::locale( decimal_mark = "." , grouping_mark = "," ) 
					)
					
				x <- data.frame( x )
				
				names( x ) <- tolower( names( x ) )
					
				# deal with decimals
				decimals <- gsub( "(.*)\\." , "" , ifelse( grepl( "\\." , txt.w ) , txt.w , "0" ) )

				for ( j in seq( txt.w ) ) if( decimals[ j ] > 0 ) x[ , j ] <- x[ , j ] / ( 10^as.numeric( decimals[j] ) )
					
				# read the data.frame `x`
				# directly into the monet database you just created.
				DBI::dbWriteTable( db , tablename , x , header = TRUE , row.names = FALSE )
		
			
			}
			
			
			if( ( i == max( which( catalog$year == catalog[ i , 'year' ] ) ) ) ){
				
				# find all tables from this catalog[ i , 'year' ]
				tables.this.year <- grep( catalog[ i , 'year' ] , DBI::dbListTables( db ) , value = TRUE )
				
				tables.this.year <- tables.this.year[ !grepl( "tour|chn" , tables.this.year ) ]
				
				# convert all tables to lowercase..
				for ( this_table in tables.this.year ){

					new.tablename <- gsub( catalog[ i , 'year' ] , paste0( '_' , catalog[ i , 'year' ] ) , this_table )

					prefix <- strsplit( new.tablename , '_' )[[1]][1]
					
					nhts_sql_process( db , this_table  , new.tablename )

				}
				
			}

			# last catalog entry of 2001?
			# remove missing weights in 2001
			if( ( 2001 %in% catalog$year ) && ( catalog[ i , 'year' ] == max( which( catalog$year == 2001 ) ) ) ){

				DBI::dbSendQuery( db , "UPDATE daypub_2001 SET wttrdntl = 0 WHERE wttrdntl IS NULL" )
				DBI::dbSendQuery( db , "UPDATE perpub_2001 SET wtprntl = 0 WHERE wtprntl IS NULL" )
				DBI::dbSendQuery( db , "UPDATE hhpub_2001 SET wthhntl = 0 WHERE wthhntl IS NULL" )
				
				for( this_wgt in grep( "wttdfn[0-9]" , DBI::dbListFields( db , 'prwt_2001' ) , value = TRUE ) ) DBI::dbSendQuery( db , paste( "UPDATE prwt_2001 SET" , this_wgt , "= 0 WHERE" , this_wgt , "IS NULL" ) )
				for( this_wgt in grep( "wtpfin[0-9]" , DBI::dbListFields( db , 'prwt_2001' ) , value = TRUE ) ) DBI::dbSendQuery( db , paste( "UPDATE prwt_2001 SET" , this_wgt , "= 0 WHERE" , this_wgt , "IS NULL" ) )
				for( this_wgt in grep( "wthfin[0-9]" , DBI::dbListFields( db , 'hhwt_2001' ) , value = TRUE ) ) DBI::dbSendQuery( db , paste( "UPDATE hhwt_2001 SET" , this_wgt , "= 0 WHERE" , this_wgt , "IS NULL" ) )
				
			}

			
			# must be the last catalog entry of the year
			
			# more stuff that's only available in the years where replicate weights
			# are freely available as csv files.
			if ( ( catalog[ i , 'year' ] > 1995 ) & ( i == max( which( catalog$year == catalog[ i , 'year' ] ) ) ) ){ 

				if ( catalog[ i , 'year' ] == 2001 ){
				
					day.table <- 'daypub'
					wt.table <- 'prwt'
					per.table <- 'perpub'
					hh.table <- 'hhpub'
					
					# replicate weight scaling factors
					sca <- 98 / 99
					rsc <- rep( 1 , 99 )
					
					# weights and replicate weights
					ldt.wt <- ~wtptpfin ; ldt.repwt <- 'fptpwt[0-9]'
					hh.wt <- ~wthhntl ; hh.repwt <- 'wthfin[0-9]'
					per.wt <- ~wtprntl ; per.repwt <- 'wtpfin[0-9]'
					day.wt <- ~wttrdntl ; day.repwt <- 'wttdfn[0-9]'
					
				} else {
				
					day.table <- 'dayvpub'
					wt.table <- 'perwt'
					per.table <- 'pervpub'
					hh.table <- 'hhvpub'

					# replicate weight scaling factors
					sca <- 99 / 100
					rsc <- rep( 1 , 100 )
					
					# weights and replicate weights
					ldt.wt <- NULL ; ldt.repwt <- NULL
					hh.wt <- ~wthhfin ; hh.repwt <- 'hhwgt[0-9]' 
					per.wt <- ~wtperfin ; per.repwt <- 'wtperfin[1-9]' 
					day.wt <- ~wttrdfin ; day.repwt <- 'daywgt[1-9]'
								
				}
			

				# last catalog entry of 2001
				if ( ( 2001 %in% catalog$year ) && ( i == max( which( catalog$year == 2001 ) ) ) ){
							
					# merge the `ldt` table with the ldt weights
					nonmatching.fields <- nhts_nmf( db , 'ldtwt' , 'ldtpub' , catalog[ i , 'year' ] )
					
					DBI::dbSendQuery( 
						db , 
						paste0(
							'create table ldt_m_' , 
							catalog[ i , 'year' ] , 
							' as select a.* , ' ,
							paste( "b." , nonmatching.fields , collapse = ", " , sep = "" ) , 
							' from ' ,
							'ldtpub' ,
							'_' ,
							catalog[ i , 'year' ] ,
							' as a inner join ' ,
							'ldtwt' ,
							'_' ,
							catalog[ i , 'year' ] ,
							' as b on a.houseid = b.houseid AND CAST( a.personid AS INTEGER ) = CAST( b.personid AS INTEGER )' 
						)
					)
					# table `ldt_m_YYYY` now available for analysis!
					nhts.ldt.design <-
						survey::svrepdesign(
							weight = ldt.wt ,
							repweights = ldt.repwt ,
							scale = sca ,
							rscales = rsc ,
							degf = 99 ,
							type = 'JK1' ,
							mse = TRUE ,
							data = paste0( 'ldt_m_' , catalog[ i , 'year' ] ) , 			# use the person-ldt-merge data table
							dbtype = "SQLite" ,
							dbname = catalog[ i , 'dbfile' ]
						)

					# workaround for a bug in survey::survey::svrepdesign.character
					nhts.ldt.design$mse <- TRUE

						
				
				}
				

				# merge the `day` table with the person-level weights
				nonmatching.fields <- nhts_nmf( db , wt.table , day.table , catalog[ i , 'year' ] )
				
				DBI::dbSendQuery( 
					db , 
					paste0(
						'create table day_m_' , 
						catalog[ i , 'year' ] , 
						' as select a.* , ' ,
						paste( "b." , nonmatching.fields , collapse = ", " , sep = "" ) , 
						' from ' ,
						day.table ,
						'_' ,
						catalog[ i , 'year' ] ,
						' as a inner join ' ,
						wt.table ,
						'_' ,
						catalog[ i , 'year' ] ,
						' as b on a.houseid = b.houseid AND CAST( a.personid AS INTEGER ) = CAST( b.personid AS INTEGER )' 
					)
				)
				# table `day_m_YYYY` now available for analysis!
				
				# immediately make the person-day-level survey::svrepdesign object.
				nhts.day.design <-
					survey::svrepdesign(
						weight = day.wt ,
						repweights = day.repwt ,
						scale = sca ,
						rscales = rsc ,
						degf = 99 ,
						type = 'JK1' ,
						mse = TRUE ,
						data = paste0( 'day_m_' , catalog[ i , 'year' ] ) , 	# use the person-day-merge data table
						dbtype = "SQLite" ,
						dbname = catalog[ i , 'dbfile' ]
					)

				# workaround for a bug in survey::survey::svrepdesign.character
				nhts.day.design$mse <- TRUE
				
					
				# merge the person table with the person-level weights
				nonmatching.fields <- nhts_nmf( db , wt.table , per.table , catalog[ i , 'year' ] )
				
				DBI::dbSendQuery( 
					db , 
					paste0(
						'create table per_m_' ,
						catalog[ i , 'year' ] ,
						' as select a.* , ' ,
						paste( "b." , nonmatching.fields , collapse = ", " , sep = "" ) , 
						' from ' ,
						per.table , 
						'_' ,
						catalog[ i , 'year' ] , 
						' as a inner join ' ,
						wt.table ,
						'_' ,
						catalog[ i , 'year' ] ,
						' as b on a.houseid = b.houseid AND CAST( a.personid AS INTEGER ) = CAST( b.personid AS INTEGER )' 
					)
				)
				# table `per_m_YYYY` now available for analysis!
				
				# immediately make the person-level survey::svrepdesign object.
				nhts.per.design <-
					survey::svrepdesign(
						weight = per.wt ,
						repweights = per.repwt ,
						scale = sca ,
						rscales = rsc ,
						degf = 99 ,
						type = 'JK1' ,
						mse = TRUE ,
						data = paste0( 'per_m_' , catalog[ i , 'year' ] ) , 	# use the person-merge data table
						dbtype = "SQLite" ,
						dbname = catalog[ i , 'dbfile' ]
					)

				# workaround for a bug in survey::survey::svrepdesign.character
				nhts.per.design$mse <- TRUE
				
			
				# merge the household table with the household-level weights
				nonmatching.fields <- nhts_nmf( db , 'hhwt' , hh.table , catalog[ i , 'year' ] )
				
				DBI::dbSendQuery( 
					db , 
					paste0(
						'create table hh_m_' ,
						catalog[ i , 'year' ] ,
						' as select a.* , ' ,
						paste( "b." , nonmatching.fields , collapse = ", " , sep = "" ) , 
						' from ' ,
						hh.table ,
						'_' ,
						catalog[ i , 'year' ] ,
						' as a inner join hhwt_' ,
						catalog[ i , 'year' ] ,
						' as b on a.houseid = b.houseid' 
					)
				)
				# table `hh_m_YYYY` now available for analysis!

				
				# immediately make the household-level survey::svrepdesign object.
				nhts.hh.design <-
					survey::svrepdesign(
						weight = hh.wt ,
						repweights = hh.repwt ,
						scale = sca ,
						rscales = rsc ,
						degf = 99 ,
						type = 'JK1' ,
						mse = TRUE ,
						data = paste0( 'hh_m_' , catalog[ i , 'year' ] ) , 	# use the household-merge data table
						dbtype = "SQLite" ,
						dbname = catalog[ i , 'dbfile' ]
					)

					
					
				# workaround for a bug in survey::survey::svrepdesign.character
				nhts.hh.design$mse <- TRUE

				# done.  phew.  save all the objects to the current working directory
				if ( catalog[ i , 'year' ] == 2001 ){

					saveRDS( nhts.ldt.design , file = paste0( catalog[ i , "output_folder" ] , "/ldt design.rds" ) , compress = FALSE )
					
				}
				
				saveRDS( nhts.per.design , file = paste0( catalog[ i , "output_folder" ] , "/per design.rds" ) , compress = FALSE )
				saveRDS( nhts.day.design , file = paste0( catalog[ i , "output_folder" ] , "/day design.rds" ) , compress = FALSE )
				saveRDS( nhts.hh.design , file = paste0( catalog[ i , "output_folder" ] , "/hh design.rds" ) , compress = FALSE )
				
				catalog[ catalog[ i , 'output_folder' ] == catalog$output_directory , 'case_count' ] <- nrow( nhts.per.design )
				
			}
						
			# delete the temporary files
			suppressWarnings( file.remove( tf , unzipped_files ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'dbfile' ] , "'\r\n\n" ) )

		}

		on.exit()
		
		catalog

	}


	
	
	
nhts_sql_process <-
	function( db , pre , post ){
			
		if ( identical( pre , post ) ) stop( "`pre` and `post` cannot be the same." )

		
		# figure out which columns are not stored as strings
		all.tables <- DBI::dbReadTable( db , 'tables' )
		
		# pull the current table's monetdb id
		this.table.id <- all.tables[ all.tables$name == pre , 'id' ]
		
		# pull all fields in this table
		all.columns <- 
			DBI::dbGetQuery( 
				db , 
				paste(
					'select * from columns where table_id = ' ,
					this.table.id
				)
			)
		
		# blank only columns that are not varchar
		cols.to.blank <- all.columns[ !( all.columns$type %in% c( 'clob' , 'varchar' , 'string' ) ) , 'name' ]
		
		
		# loop through every field in the data set
		# and blank out all negative numbers
		for ( j in cols.to.blank ) DBI::dbSendQuery( db , paste( 'UPDATE' , pre , 'SET' , j , '= NULL WHERE' , j , '< 0' ) )
		
		# get rid of `id9` field #
		lowered.edited.fields <- tolower( DBI::dbListFields( db , pre ) )
		
		if ( lowered.edited.fields[ 1 ] == 'id9' ) lowered.edited.fields[ 1 ] <- 'houseid'
		
		casting.chars <- DBI::dbListFields( db , pre )
		casting.chars <- gsub( "houseid" , "CAST( LTRIM( RTRIM( CAST( houseid AS STRING ) ) ) AS STRING )" , casting.chars )
		casting.chars <- gsub( "personid" , "CAST( personid AS INTEGER )" , casting.chars )
		casting.chars <- gsub( "id9" , "CAST( LTRIM( RTRIM( CAST( id9 AS STRING ) ) ) AS STRING )" , casting.chars )
		
		
		
		# build the 'create table' sql command
		sql.create.table <-
			paste(
				'create table' , 
				post ,
				'as select' ,

				paste(
					# select all fields in the data set..
					casting.chars ,
					# re-select them, but convert them to lowercase
					lowered.edited.fields , 
					# separate them by `as` statements
					sep = ' as ' ,
					# and mush 'em all together with commas
					collapse = ', '
				) ,
				
				# tack on a column of all ones
				', 1 as one from' ,
				 
				pre ,
				
				''
			)

		# actually execute the create table command
		DBI::dbSendQuery( db , sql.create.table )

		# remove the source data table
		DBI::dbRemoveTable( db , pre )
	}
# function end


# initiate a function
# that takes two tables (named by a _year pattern)
# and returns the non-intersecting field names of the b.table
# this will be used for monetdb sql joins
nhts_nmf <- 
			function( conn , b.table , a.table , yr ){
				DBI::dbListFields( 
					conn , 
					paste0( b.table , '_' , yr ) )[ 
						!( 
							DBI::dbListFields( conn , paste0( b.table , '_' , yr ) ) %in% DBI::dbListFields( conn , paste0( a.table , '_' , yr ) ) 
						) 
					]
			}
# function end
