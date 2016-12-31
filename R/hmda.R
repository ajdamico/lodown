get_catalog_hmda <-
	function( data_name = "hmda" , output_dir , ... ){

		hmda_table <- rvest::html_table( xml2::read_html( "https://www.ffiec.gov/hmda/hmdaflat.htm" ) , fill = TRUE )
		
		pmic_table <- rvest::html_table( xml2::read_html( "https://www.ffiec.gov/hmda/pmicflat.htm" ) , fill = TRUE )
		
		latest_hmda_year <- max( unlist( sapply( hmda_table , function( z ) suppressWarnings( as.numeric( names( z ) ) ) ) ) , na.rm = TRUE )
		
		latest_pmic_year <- max( unlist( sapply( pmic_table , function( z ) suppressWarnings( as.numeric( names( z ) ) ) ) ) , na.rm = TRUE )
		
		cat_hmda_lar <-
			data.frame(
				year = 2006:latest_hmda_year ,
				type = 'hmda_lar' ,
				full_url = paste0( "https://www.ffiec.gov/" , "hmda" , "rawdata/" , "LAR/National" , "/" , 2006:latest_hmda_year , "HMDA" , "lar%20-%20National" , ".zip" ) ,
				stringsAsFactors = FALSE
			)
			
		cat_pmic_lar <-
			data.frame(
				year = 2006:latest_pmic_year ,
				type = 'pmic_lar' ,
				full_url = paste0( "https://www.ffiec.gov/" , "pmic" , "rawdata/" , "LAR/National" , "/" , 2006:latest_pmic_year , "PMIC" , "lar%20-%20National" , ".zip" ) ,
				stringsAsFactors = FALSE
			)
			
		cat_hmda_inst <-
			data.frame(
				year = 2006:latest_hmda_year ,
				type = 'hmda_inst' ,
				full_url = paste0( "https://www.ffiec.gov/" , "hmda" , "rawdata/" , "OTHER" , "/" , 2006:latest_hmda_year , "HMDA" , "institutionrecords" , ".zip" ) ,
				stringsAsFactors = FALSE
			)
			
		cat_pmic_inst <-
			data.frame(
				year = 2006:latest_pmic_year ,
				type = 'pmic_inst' ,
				full_url = paste0( "https://www.ffiec.gov/" , "pmic" , "rawdata/" , "OTHER" , "/" , 2006:latest_pmic_year , "PMIC" , "institutionrecords" , ".zip" ) ,
				stringsAsFactors = FALSE
			)
			
		cat_hmda_reporter <-
			data.frame(
				year = 2007:latest_hmda_year ,
				type = 'hmda_reporter' ,
				full_url = paste0( "https://www.ffiec.gov/" , "hmda" , "rawdata/OTHER/" , 2007:latest_hmda_year , "HMDA" , "ReporterPanel.zip" ) ,
				stringsAsFactors = FALSE
			)
			
		cat_pmic_reporter <-
			data.frame(
				year = 2007:latest_pmic_year ,
				type = 'pmic_reporter' ,
				full_url = paste0( "https://www.ffiec.gov/" , "pmic" , "rawdata/OTHER/" , 2007:latest_pmic_year , "PMIC" , "ReporterPanel.zip" ) ,
				stringsAsFactors = FALSE
			)
			
		
		cat_hmda_msa <-
			data.frame(
				year = 2007:latest_hmda_year ,
				type = 'hmda_msa' ,
				full_url = paste0( "https://www.ffiec.gov/" , "hmda" , "rawdata/OTHER/" , 2007:latest_hmda_year , "HMDA" , "MSAOffice.zip" ) ,
				stringsAsFactors = FALSE
			)
			
		cat_pmic_msa <-
			data.frame(
				year = 2007:latest_pmic_year ,
				type = 'pmic_msa' ,
				full_url = paste0( "https://www.ffiec.gov/" , "hmda" , "rawdata/OTHER/" , 2007:latest_pmic_year , "HMDA" , "MSAOffice.zip" ) ,
				stringsAsFactors = FALSE
			)
			
		catalog <- rbind( cat_hmda_lar , cat_pmic_lar , cat_hmda_inst , cat_pmic_inst , cat_hmda_reporter , cat_pmic_reporter , cat_hmda_msa , cat_pmic_msa )

		catalog$dbfolder <- paste0( output_dir , "/MonetDB" )
		
		catalog$db_tablename <- paste0( catalog$type , "_" , catalog$year )
		
		catalog <- catalog[ order( catalog$year ) , ]
		
		catalog
	}


lodown_hmda <-
	function( data_name = "hmda" , catalog , path_to_7za = '7za' , ... ){

		if( ( .Platform$OS.type != 'windows' ) && ( system( paste0('"', path_to_7za , '" -h' ) , show.output.on.console = FALSE ) != 0 ) ) stop( "you need to install 7-zip.  if you already have it, include a path_to_7za='/directory/7za' parameter" )

		tf <- tempfile()
		
		ins_sas <- system.file("extdata", "ins_str.csv", package = "lodown")
		lar_sas <- system.file("extdata", "lar_str.csv", package = "lodown")
		rp_sas <- system.file("extdata", "Reporter_Panel_2010.sas", package = "lodown")
		pr_str <- system.file("extdata", "Reporter_Panel_Pre-2010.sas", package = "lodown")

		# read in the loan application record structure file
		lar_str <- read.csv( lar_sas )
		# paste all rows together into single strings
		lar_col <- apply( lar_str , 1 , paste , collapse = " " )

		# take a look at the `lar_str` and `lar_col` objects if you're curious
		# just type 'em into the console to see what i mean ;)

		# read in the loan application record structure file
		ins_str <- read.csv( ins_sas )
		# paste all rows together into single strings
		ins_col <- apply( ins_str , 1 , paste , collapse = " " )

				
		# create an msa office sas importation script..
		office.lines <- 
			"INPUT
			AS_OF_YEAR 4
			Agency_Code $ 1
			Respondent_ID $ 10
			MSA_MD $ 5
			MSA_MD_Description $ 50 
			;"
			
		# ..save it to the local disk as a temporary file..
		writeLines( office.lines , tf )

		# ..and save the column names into a new object `office.names`
		office.names <- tolower( SAScii::parse.SAScii( tf )$varname )



		for ( i in seq_len( nrow( catalog ) ) ){

			# open the connection to the monetdblite database
			db <- DBI::dbConnect( MonetDBLite::MonetDBLite() , catalog[ i , 'dbfolder' ] )

			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

			
			if ( .Platform$OS.type == 'windows' ){

				unzipped_files <- unzip( tf , exdir = paste0( tempdir() , "/unzips" ) , overwrite = TRUE )

			} else {
			
				files_before <- list.files( paste0( tempdir() , "/unzips" ) , full.names = TRUE )

				# build the string to send to the terminal on non-windows systems
				dos_command <- paste0( '"' , path_to_7za , '" x ' , tf , ' -aoa -o"' , paste0( tempdir() , "/unzips" ) , '"' )

				system( dos_command )

				unzipped_files <- list.files( paste0( tempdir() , "/unzips" ) , full.names = TRUE )

				unzipped_files <- unzipped_files[ !( unzipped_files %in% files_before ) ]
				
			}
			
			if( length( unzipped_files ) != 1 ) stop( "always expecting a single import file" )

			if( grepl( "reporter" , catalog[ i , 'type' ] ) ){
		
				if( catalog[ i , 'year' ] < 2010 ) sas_ri <- pr_str else sas_ri <- rp_sas
				
				# read that temporary file directly into MonetDB,
				# using only the sas importation script
				read_SAScii_monetdb (
					unzipped_files ,			# the url of the file to download
					sas_ri ,			# the 
					zipped = FALSE ,	# the ascii file is stored in a zipped file
					tl = TRUE ,		# convert all column names to lowercase
					tablename = catalog[ i , 'db_tablename' ] ,
					connection = db
				)

			}
			
			if( grepl( "msa" , catalog[ i , 'type' ] ) ){
				
				# read the entire file into RAM
				msa_ofc <-
					read.table(
						unzipped_files ,
						header = FALSE ,
						quote = "\"" ,
						sep = '\t' ,
						# ..using the `office.names` extracted from the code above
						col.names = office.names
					)
					
				names( msa_ofc ) <- tolower( names( msa_ofc ) )
					
				# write the `msa` table into the database directly
				DBI::dbWriteTable( db , catalog[ i , 'db_tablename' ] , msa_ofc )
				
			}
			
			
					
			# cycle through both institutional records and loan applications received microdata files
			if ( grepl( "inst|lar" , catalog[ i , 'type' ] ) ){

				if( grepl( "inst" , catalog[ i , 'type' ] ) ) rectype <- "institutionrecords"
				
				if( grepl( "lar" , catalog[ i , 'type' ] ) ) rectype <- "lar%20-%20National"
				
			
				# strip just the first three characters from `rectype`
				short.name <- substr( rectype , 1 , 3 )
			
				# pull the structure construction
				col_str <- get( paste( short.name , "col" , sep = "_" ) )
				
				# design the monetdb table
				sql.create <- sprintf( paste( "CREATE TABLE" , catalog[ i , 'db_tablename' ] , "(%s)" ) , paste( col_str , collapse = ", " ) )

				# initiate the monetdb table
				DBI::dbSendQuery( db , sql.create )

				# find the url folder and the appropriate delimiter line for the monetdb COPY INTO command
				if ( short.name == "lar" ){
				
					delim.line <- "' using delimiters ',','\\n','\"' NULL AS ''" 
					
				} else {
				
					delim.line <- "' using delimiters '\\t' NULL AS ''" 
				
				}
				
				
				# construct the monetdb COPY INTO command
				sql.copy <- 
					paste0( 
						"copy into " , 
						catalog[ i , 'db_tablename' ] , 
						" from '" , 
						normalizePath( unzipped_files ) , 
						delim.line
					)
					
				# actually execute the COPY INTO command
				DBI::dbSendQuery( db , sql.copy )
			
				# conversion of numeric columns incorrectly stored as character strings #
			
				# initiate a character vector containing all columns that should be numeric types
				revision.variables <- c( "sequencenumber" , "population" , "minoritypopulationpct" , "hudmedianfamilyincome" , "tracttomsa_mdincomepct" , "numberofowneroccupiedunits" , "numberof1to4familyunits" )

				# determine whether any of those variables are in the current table
				field.revisions <- DBI::dbListFields( db , catalog[ i , 'db_tablename' ] )[ tolower( DBI::dbListFields( db , catalog[ i , 'db_tablename' ] ) ) %in% revision.variables ]

				# loop through each of those variables
				for ( col.rev in field.revisions ){

					# add a new `temp_double` column in the data table
					DBI::dbSendQuery( db , paste( "ALTER TABLE" , catalog[ i , 'db_tablename' ] , "ADD COLUMN temp_double DOUBLE" ) )

					# copy over the contents of the character-typed column so long as the column isn't a textual missing
					DBI::dbSendQuery( db , paste( "UPDATE" , catalog[ i , 'db_tablename' ] , "SET temp_double = CAST(" , col.rev , " AS DOUBLE ) WHERE TRIM(" , col.rev , ") <> 'NA'" ) )
					
					# remove the character-typed column from the data table
					DBI::dbSendQuery( db , paste( "ALTER TABLE" , catalog[ i , 'db_tablename' ] , "DROP COLUMN" , col.rev ) )
					
					# re-initiate the same column name, but as a numeric type
					DBI::dbSendQuery( db , paste( "ALTER TABLE" , catalog[ i , 'db_tablename' ] , "ADD COLUMN" , col.rev , "DOUBLE" ) )
					
					# copy the corrected contents back to the original column name
					DBI::dbSendQuery( db , paste( "UPDATE" , catalog[ i , 'db_tablename' ] , "SET" , col.rev , "= temp_double" ) )
					
					# remove the temporary column from the data table
					DBI::dbSendQuery( db , paste( "ALTER TABLE" , catalog[ i , 'db_tablename' ] , "DROP COLUMN temp_double" ) )

				}

				# end of conversion of numeric columns incorrectly stored as character strings #

			}
			
			


			
			stopifnot( DBI::dbGetQuery( db , paste( 'select count(*) from' , catalog[ i , 'db_tablename' ] ) ) > 0 )

			# disconnect from the current monet database
			DBI::dbDisconnect( db , shutdown = TRUE )
			
			# delete the temporary files
			suppressWarnings( file.remove( tf , unzipped_files , list.files( paste0( tempdir() , "/unzips" ) , full.names = TRUE ) ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'db_tablename' ] , "'\r\n\n" ) )

		}

		invisible( TRUE )

	}

