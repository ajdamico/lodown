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
				merge_table = paste0( 'hmda_' , 2006:latest_hmda_year ) ,
				stringsAsFactors = FALSE
			)
			
		cat_pmic_lar <-
			data.frame(
				year = 2006:latest_pmic_year ,
				type = 'pmic_lar' ,
				full_url = paste0( "https://www.ffiec.gov/" , "pmic" , "rawdata/" , "LAR/National" , "/" , 2006:latest_pmic_year , "PMIC" , "lar%20-%20National" , ".zip" ) ,
				merge_table = paste0( 'pmic_' , 2006:latest_pmic_year ) ,
				stringsAsFactors = FALSE
			)
			
		cat_hmda_inst <-
			data.frame(
				year = 2006:latest_hmda_year ,
				type = 'hmda_inst' ,
				full_url = paste0( "https://www.ffiec.gov/" , "hmda" , "rawdata/" , "OTHER" , "/" , 2006:latest_hmda_year , "HMDA" , "institutionrecords" , ".zip" ) ,
				merge_table = paste0( 'hmda_' , 2006:latest_hmda_year ) ,
				stringsAsFactors = FALSE
			)
			
		cat_pmic_inst <-
			data.frame(
				year = 2006:latest_pmic_year ,
				type = 'pmic_inst' ,
				full_url = paste0( "https://www.ffiec.gov/" , "pmic" , "rawdata/" , "OTHER" , "/" , 2006:latest_pmic_year , "PMIC" , "institutionrecords" , ".zip" ) ,
				merge_table = paste0( 'pmic_' , 2006:latest_pmic_year ) ,
				stringsAsFactors = FALSE
			)
			
		cat_hmda_reporter <-
			data.frame(
				year = 2007:latest_hmda_year ,
				type = 'hmda_reporter' ,
				full_url = paste0( "https://www.ffiec.gov/" , "hmda" , "rawdata/OTHER/" , 2007:latest_hmda_year , "HMDA" , "ReporterPanel.zip" ) ,
				merge_table = NA ,
				stringsAsFactors = FALSE
			)
			
		cat_pmic_reporter <-
			data.frame(
				year = 2007:latest_pmic_year ,
				type = 'pmic_reporter' ,
				full_url = paste0( "https://www.ffiec.gov/" , "pmic" , "rawdata/OTHER/" , 2007:latest_pmic_year , "PMIC" , "ReporterPanel.zip" ) ,
				merge_table = NA ,
				stringsAsFactors = FALSE
			)
			
		
		cat_hmda_msa <-
			data.frame(
				year = 2007:latest_hmda_year ,
				type = 'hmda_msa' ,
				full_url = paste0( "https://www.ffiec.gov/" , "hmda" , "rawdata/OTHER/" , 2007:latest_hmda_year , "HMDA" , "MSAOffice.zip" ) ,
				merge_table = NA ,
				stringsAsFactors = FALSE
			)
			
		cat_pmic_msa <-
			data.frame(
				year = 2007:latest_pmic_year ,
				type = 'pmic_msa' ,
				full_url = paste0( "https://www.ffiec.gov/" , "hmda" , "rawdata/OTHER/" , 2007:latest_pmic_year , "HMDA" , "MSAOffice.zip" ) ,
				merge_table = NA ,
				stringsAsFactors = FALSE
			)
			
		catalog <- rbind( cat_hmda_lar , cat_pmic_lar , cat_hmda_inst , cat_pmic_inst , cat_hmda_reporter , cat_pmic_reporter , cat_hmda_msa , cat_pmic_msa )

		catalog[ , 'dbfile' ] <- paste0( output_dir , "/SQLite.db" )
		
		catalog$db_tablename <- paste0( catalog$type , "_" , catalog$year )
		
		catalog <- catalog[ order( catalog$year ) , ]
		
		catalog
	}


lodown_hmda <-
	function( data_name = "hmda" , catalog , path_to_7za = '7za' , ... ){

		if( ( .Platform$OS.type != 'windows' ) && ( system( paste0('"', path_to_7za , '" -h' ) ) != 0 ) ) stop( "you need to install 7-zip.  if you already have it, include a path_to_7za='/directory/7za' parameter" )
		
		on.exit( print( catalog ) )

		tf <- tempfile()
		
		ins_sas <- system.file("extdata", "hmda/ins_str.csv", package = "lodown")
		lar_sas <- system.file("extdata", "hmda/lar_str.csv", package = "lodown")
		rp_sas <- system.file("extdata", "hmda/Reporter_Panel_2010.sas", package = "lodown")
		pr_str <- system.file("extdata", "hmda/Reporter_Panel_Pre-2010.sas", package = "lodown")

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
		suppressWarnings( office.names <- tolower( SAScii::parse.SAScii( tf )$varname ) )



		for ( i in seq_len( nrow( catalog ) ) ){

			# open the connection to the monetdblite database
			db <- DBI::dbConnect( RSQLite::SQLite() , catalog[ i , 'dbfile' ] )

			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )
	
			if ( .Platform$OS.type == 'windows' ){

				unzipped_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , "/unzips" ) , overwrite = TRUE )

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
				
				
				if( catalog[ i , 'db_tablename' ] == "hmda_inst_2016" ){
				
					this_table <- read.table( unzipped_files , sep = '\t' , head = FALSE , comment.char = '' , quote = '' , fill = TRUE )
					names( this_table ) <- DBI::dbListFields( db , catalog[ i , 'db_tablename' ] )
					DBI::dbRemoveTable( db , catalog[ i , 'db_tablename' ] )
					DBI::dbWriteTable( db , catalog[ i , 'db_tablename' ] , this_table )
				
				} else {
					
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
				
				}
				
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

			catalog[ i , 'case_count' ] <- DBI::dbGetQuery( db , paste( 'select count(*) from' , catalog[ i , 'db_tablename' ] ) )[ 1 , 1 ]
			
			stopifnot( catalog[ i , 'case_count' ] > 0 )

			# delete the temporary files
			suppressWarnings( file.remove( tf , unzipped_files , list.files( paste0( tempdir() , "/unzips" ) , full.names = TRUE ) ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'db_tablename' ] , "'\r\n\n" ) )

		}

		
		unique_merge_tables <- unique( catalog$merge_table )
		
		unique_merge_tables <- unique_merge_tables[ !is.na( unique_merge_tables ) ]
		
		for( i in seq_along( unique_merge_tables ) ){
			
			# open the connection to the monetdblite database
			db <- DBI::dbConnect( RSQLite::SQLite() , unique( catalog[ which( catalog$merge_table == unique_merge_tables[ i ] ) , 'dbfile' ] ) )

			ins.tablename <- catalog[ substr( catalog$type , 6 , 8 ) == 'ins' & catalog$merge_table == unique_merge_tables[ i ] , 'db_tablename' ]
			lar.tablename <- catalog[ substr( catalog$type , 6 , 8 ) == 'lar' & catalog$merge_table == unique_merge_tables[ i ] , 'db_tablename' ]
			new.tablename <- unique_merge_tables[ i ]
			
			# three easy steps #
			
			# step one: confirm the only intersecting fields are "respondentid" and "agencycode"
			# these are the merge fields, so nothing else can overlap
			
			stopifnot( 
				identical( 
					intersect( 
						DBI::dbListFields( db , lar.tablename ) , 
						DBI::dbListFields( db , ins.tablename ) 
					) , 
					c( 'respondentid' , 'agencycode' ) 
				) 
			)
			
			# step two: merge the two tables
			
			# extract the column names from the institution table
			ins.fields <- DBI::dbListFields( db , ins.tablename )
			
			# throw out the two merge fields
			ins.nomatch <- ins.fields[ !( ins.fields %in% c( 'respondentid' , 'agencycode' ) ) ]
			
			# add a "b." in front of every field name
			ins.b <- paste0( "b." , ins.nomatch )
			
			# separate all of them by commas into a single character string
			ins.string <- paste( ins.b , collapse = ", " )
			
			# construct the merge command
			sql.merge.command <-
				paste(
					"CREATE TABLE" , 
					new.tablename ,
					"AS SELECT a.* ," ,
					ins.string ,
					"FROM" ,
					lar.tablename ,
					"AS a INNER JOIN" ,
					ins.tablename ,
					"AS b ON a.respondentid = b.respondentid AND a.agencycode = b.agencycode"
				)
			
			# with your sql string built, execute the command
			DBI::dbSendQuery( db , sql.merge.command )
			
			# step three: confirm that the merged table contains the same record count
			stopifnot( 
				DBI::dbGetQuery( 
					db , 
					paste(
						'select count(*) from' ,
						new.tablename
					)
				) ==
				DBI::dbGetQuery( 
					db , 
					paste(
						'select count(*) from' ,
						lar.tablename
					)
				)
			)

			# # # # # # # # # # # # # # # # # #
			# # race and ethnicity recoding # #
			# # # # # # # # # # # # # # # # # #
			
			# number of minority races of applicant and co-applicant
			DBI::dbSendQuery( db , paste( 'ALTER TABLE' , new.tablename , 'ADD COLUMN app_min_cnt INTEGER' ) )
			DBI::dbSendQuery( db , paste( 'ALTER TABLE' , new.tablename , 'ADD COLUMN co_min_cnt INTEGER' ) )

			# sum up all four possibilities
			DBI::dbSendQuery( 
				db , 
				paste(
					'UPDATE' ,
					new.tablename ,
					'SET 
						app_min_cnt = 
						(
							( applicantrace1 IN ( 1 , 2 , 3 , 4 ) )*1 +
							( applicantrace2 IN ( 1 , 2 , 3 , 4 ) )*1 +
							( applicantrace3 IN ( 1 , 2 , 3 , 4 ) )*1 +
							( applicantrace4 IN ( 1 , 2 , 3 , 4 ) )*1 +
							( applicantrace5 IN ( 1 , 2 , 3 , 4 ) )*1
						)' 
				)
			)

			# same for the co-applicant
			DBI::dbSendQuery( 
				db , 
				paste(
					'UPDATE' ,
					new.tablename , 
					'SET 
						co_min_cnt = 
						(
							( coapplicantrace1 IN ( 1 , 2 , 3 , 4 ) )*1 +
							( coapplicantrace2 IN ( 1 , 2 , 3 , 4 ) )*1 +
							( coapplicantrace3 IN ( 1 , 2 , 3 , 4 ) )*1 +
							( coapplicantrace4 IN ( 1 , 2 , 3 , 4 ) )*1 +
							( coapplicantrace5 IN ( 1 , 2 , 3 , 4 ) )*1
						)' 
				)
			)

			# zero-one test of whether the applicant or co-applicant indicated white
			DBI::dbSendQuery( db , paste( 'ALTER TABLE' , new.tablename , 'ADD COLUMN appwhite INTEGER' ) )
			DBI::dbSendQuery( db , paste( 'ALTER TABLE' , new.tablename , 'ADD COLUMN cowhite INTEGER' ) )

			# check all five race categories for the answer
			DBI::dbSendQuery( 
				db , 
				paste(
					'UPDATE' ,
					new.tablename , 
					'SET 
						appwhite = 
						( 
							( ( applicantrace1 ) IN ( 5 ) )*1 + 
							( ( applicantrace2 ) IN ( 5 ) )*1 + 
							( ( applicantrace3 ) IN ( 5 ) )*1 + 
							( ( applicantrace4 ) IN ( 5 ) )*1 + 
							( ( applicantrace5 ) IN ( 5 ) )*1 
						)' 
				)
			)

			# same for the co-applicant
			DBI::dbSendQuery( 
				db , 
				paste(
					'UPDATE' ,
					new.tablename ,
					'SET 
						cowhite = 
						( 
							( ( coapplicantrace1 ) IN ( 5 ) )*1 + 
							( ( coapplicantrace2 ) IN ( 5 ) )*1 + 
							( ( coapplicantrace3 ) IN ( 5 ) )*1 + 
							( ( coapplicantrace4 ) IN ( 5 ) )*1 + 
							( ( coapplicantrace5 ) IN ( 5 ) )*1 
						)' 
				)
			)

			# if the applicant or co-applicant has a missing first race, set the above variables to missing as well
			DBI::dbSendQuery( db , paste( 'UPDATE' , new.tablename , 'SET app_min_cnt = NULL WHERE applicantrace1 IN ( 6 , 7 )' ) )
			DBI::dbSendQuery( db , paste( 'UPDATE' , new.tablename , 'SET appwhite = NULL WHERE applicantrace1 IN ( 6 , 7 )' ) )
			DBI::dbSendQuery( db , paste( 'UPDATE' , new.tablename , 'SET co_min_cnt = NULL WHERE coapplicantrace1 IN ( 6 , 7 , 8 )' ) )
			DBI::dbSendQuery( db , paste( 'UPDATE' , new.tablename , 'SET cowhite = NULL WHERE coapplicantrace1 IN ( 6 , 7 , 8 )' ) )

			# main race variable
			DBI::dbSendQuery( db , paste( 'ALTER TABLE' , new.tablename , 'ADD COLUMN race INTEGER' ) )

			# 7 indicates a loan by a white applicant and non-white co-applicant or vice-versa
			DBI::dbSendQuery( db , paste( 'UPDATE' , new.tablename , 'SET race = 7 WHERE ( appwhite = 1 AND app_min_cnt = 0 AND co_min_cnt > 0 ) OR ( cowhite = 1 AND co_min_cnt = 0 AND app_min_cnt > 0 )' ) )

			# 6 indicates the main applicant listed multiple non-white races
			DBI::dbSendQuery( db , paste( 'UPDATE' , new.tablename , 'SET race = 6 WHERE ( app_min_cnt > 1 ) AND ( race IS NULL )' ) )

			# for everybody else: if the first race listed by the applicant isn't white, use that.
			DBI::dbSendQuery( db , paste( "UPDATE" , new.tablename , "SET race = applicantrace1 WHERE ( applicantrace1 IN ( '1' , '2' , '3' , '4' ) ) AND ( app_min_cnt = 1 ) AND ( race IS NULL )" ) )
			# otherwise look to the second listed race
			DBI::dbSendQuery( db , paste( "UPDATE" , new.tablename , "SET race = applicantrace2 WHERE ( applicantrace2 IN ( '1' , '2' , '3' , '4' ) ) AND ( app_min_cnt = 1 ) AND ( race IS NULL )" ) )
			# otherwise confirm the applicant indicated he or she was white
			DBI::dbSendQuery( db , paste( 'UPDATE' , new.tablename , "SET race = 5 WHERE ( appwhite = 1 ) AND ( race IS NULL )" ) )

			# main ethnicity variable
			DBI::dbSendQuery( db , paste( 'ALTER TABLE' , new.tablename , 'ADD COLUMN ethnicity VARCHAR (255)' ) )

			# simple.  check the applicant's ethnicity
			DBI::dbSendQuery( db , paste( "UPDATE" , new.tablename , "SET ethnicity = 'Not Hispanic' WHERE applicantethnicity IN ( 2 )" ) )
			
			# simple.  check the applicant's ethnicity again
			DBI::dbSendQuery( db , paste( "UPDATE" , new.tablename , "SET ethnicity = 'Hispanic' WHERE applicantethnicity IN ( 1 )" ) )
			
			# overwrite the ethnicity variable if the main applicant indicates hispanic but the co-applicant does not.  or vice versa.
			DBI::dbSendQuery( db , paste( "UPDATE" , new.tablename , "SET ethnicity = 'Joint' WHERE ( applicantethnicity IN ( 1 ) AND coapplicantethnicity IN ( 2 ) ) OR ( applicantethnicity IN ( 2 ) AND coapplicantethnicity IN ( 1 ) )" ) )

			# # # # # # # # # # # # # # # # # # # # # # # # #
			# # finished with race and ethnicity recoding # #
			# # # # # # # # # # # # # # # # # # # # # # # # #
			
			
		}

		on.exit()
		
		catalog

	}

