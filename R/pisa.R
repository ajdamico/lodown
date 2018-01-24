get_catalog_pisa <-
	function( data_name = "pisa" , output_dir , ... ){

		if ( !requireNamespace( "archive" , quietly = TRUE ) ) stop( "archive needed for this function to work. to install it, type `devtools::install_github( 'jimhester/archive' )`" , call. = FALSE )

	
		http.pre <- "https://www.oecd.org/pisa/pisaproducts/"

		files_2015 <- c( "CMB_STU_QQQ" , "CMB_SCH_QQQ" , "CMB_TCH_QQQ" , "CMB_STU_COG" , "CMB_STU_QTM" , rep( "CM2_STU_QQQ_COG_QTM_SCH_TCH" , 5 ) )
		tables_2015 <- c( "CMB_STU_QQQ" , "CMB_SCH_QQQ" , "CMB_TCH_QQQ" , "CMB_STU_COG" , "CMB_STU_QTM" , "CY6_MS_CM2_SCH_QQQ" , "CY6_MS_CM2_STU_COG" , "CY6_MS_CM2_STU_QQQ" , "CY6_MS_CM2_STU_QTM" , "CY6_MS_CM2_TCH_QQQ" )
	
		cat_2015 <-
			data.frame(
				dbfile = paste0( output_dir , "/SQLite.db" ) ,
				db_tablename = paste0( tolower( tables_2015 ) , "_2015" ),
				year = 2015 ,
				full_url = paste0( "http://vs-web-fs-1.oecd.org/pisa/PUF_SAS_COMBINED_" , files_2015 , ".zip" ) ,
				sas_ri = NA ,
				design = c( paste0( output_dir , "/2015 " , "cmb_stu_qqq" , " design.rds" ) , rep( NA , 9 ) ) ,
				stringsAsFactors = FALSE
			)

			
		files_2012 <- c( "INT_STU12_DEC03", "INT_SCQ12_DEC03" ,  "INT_PAQ12_DEC03" , "INT_COG12_DEC03" , "INT_COG12_S_DEC03" )
		
		sas_2012 <- paste0( "PISA" , 2012 , "_SAS_" , c( "student" , "school" , "parent" , "cognitive_item" , "scored_cognitive_item" ) )
		
		cat_2012 <-
			data.frame(
				dbfile = paste0( output_dir , "/SQLite.db" ) ,
				db_tablename = tolower( files_2012 ) ,
				year = 2012 ,
				full_url = paste0( http.pre , files_2012 , ".zip" ) ,
				sas_ri = paste0( http.pre , sas_2012 , ".sas" ) ,
				design = c( paste0( output_dir , "/2012 " , "int_stu12_dec03" , " design.rds" ) , rep( NA , 4 ) ) ,
				stringsAsFactors = FALSE
			)
			
		files_2009 <- c( "INT_STQ09_DEC11" , "INT_SCQ09_Dec11" , "INT_PAR09_DEC11" , "INT_COG09_TD_DEC11" , "INT_COG09_S_DEC11" )
	
		sas_2009 <- paste0( "PISA" , 2009 , "_SAS_" , c( "student" , "school" , "parent" , "cognitive_item" , "scored_cognitive_item" ) )
	
		cat_2009 <-
			data.frame(
				dbfile = paste0( output_dir , "/SQLite.db" ) ,
				db_tablename = tolower( files_2009 ) ,
				year = 2009 ,
				full_url = paste0( http.pre , files_2009 , ".zip" ) ,
				sas_ri = paste0( http.pre , sas_2009 , ".sas" ) ,
				design = c( paste0( output_dir , "/2009 " , "int_stq09_dec11" , " design.rds" ) , rep( NA , 4 ) ) ,
				stringsAsFactors = FALSE
			)
			

		files_2006 <- c( "INT_Stu06_Dec07" , "INT_Sch06_Dec07" , "INT_Par06_Dec07" , "INT_Cogn06_T_Dec07" , "INT_Cogn06_S_Dec07" )

		sas_2006 <- paste0( "PISA" , 2006 , "_SAS_" , c( "student" , "school" , "parent" , "cognitive_item" , "scored_cognitive_item" ) )
		
		cat_2006 <-
			data.frame(
				dbfile = paste0( output_dir , "/SQLite.db" ) ,
				db_tablename = tolower( files_2006 ) ,
				year = 2006 ,
				full_url = paste0( http.pre , files_2006 , ".zip" ) ,
				sas_ri = paste0( http.pre , sas_2006 , ".sas" ) ,
				design = c( paste0( output_dir , "/2006 " , "int_stu06_dec07" , " design.rds" ) , rep( NA , 4 ) ) ,
				stringsAsFactors = FALSE
			)
			
		
		files_2003 <- c( "INT_cogn_2003" , "INT_stui_2003_v2" , "INT_schi_2003" )
	
		sas_2003 <- paste0( "PISA" , 2003 , "_SAS_" , c( "cognitive_item" , "student" , "school" ) )
	
		cat_2003 <-
			data.frame(
				dbfile = paste0( output_dir , "/SQLite.db" ) ,
				db_tablename = tolower( files_2003 ) ,
				year = 2003 ,
				full_url = paste0( http.pre , files_2003 , ".zip" ) ,
				sas_ri = paste0( http.pre , sas_2003 , ".sas" ) ,
				design = c( NA , paste0( output_dir , "/2003 " , "int_cogn_2003" , " design.rds" )  , NA ) ,
				stringsAsFactors = FALSE
			)
			
		files_2000 <- c( "intcogn_v4" , "intscho" , "intstud_math" , "intstud_read" , "intstud_scie" )

		sas_2000 <- paste0( "PISA" , 2000 , "_SAS_" , c( "cognitive_item" , "school_questionnaire" , "student_mathematics" , "student_reading" , "student_science" ) )
	
	
		cat_2000 <-
			data.frame(
				dbfile = paste0( output_dir , "/SQLite.db" ) ,
				db_tablename = tolower( files_2000 ) ,
				year = 2000 ,
				full_url = paste0( http.pre , files_2000 , ".zip" ) ,
				sas_ri = paste0( http.pre , sas_2000 , ".sas" ) ,
				design = c( NA , NA , paste0( output_dir , "/2000 " , c( "instud_math" , "intstud_read" , "intstud_scie" ) , " design.rds" ) ) ,
				stringsAsFactors = FALSE
			)
			
			
			
		rbind( cat_2015 , cat_2012 , cat_2009 , cat_2006 , cat_2003 , cat_2000 )

	}


lodown_pisa <-
	function( data_name = "pisa" , catalog , ... ){

		on.exit( print( catalog ) )

		if ( !requireNamespace( "mitools" , quietly = TRUE ) ) stop( "mitools needed for this function to work. to install it, type `install.packages( 'mitools' )`" , call. = FALSE )
				
		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			# open the connection to the monetdblite database
			db <- DBI::dbConnect( RSQLite::SQLite() , catalog[ i , 'dbfile' ] )

			tables_before <- DBI::dbListTables( db )

			if( catalog[ i , 'year' ] >= 2015 ){
			
				cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

				archive::archive_extract( tf , dir = paste0( tempdir() , "/unzips" ) )

				unzipped_files <- list.files( paste0( tempdir() , "/unzips" ) , full.names = TRUE )

				# loop through all sas7bdat files and load them into monetdb
				for( this_sas in grep( "\\.sas7bdat$" , unzipped_files , value = TRUE ) ){
				
					this_df <- data.frame( haven::read_sas( this_sas ) )
					
					names( this_df ) <- tolower( names( this_df ) )
					
					DBI::dbWriteTable( db , catalog[ i , 'db_tablename' ] , this_df )
					
				}
			
				if( !is.na( catalog[ i , 'design' ] ) ){

					# use the table (already imported into monetdb) to spawn ten different tables (one for each plausible [imputed] value)
					# then construct a multiply-imputed, monetdb-backed, replicated-weighted complex-sample survey-design object-object.
					pisa_construct.pisa.survey.designs(
						db , 
						year = catalog[ i , 'year' ] ,
						table.name = catalog[ i , 'db_tablename' ] ,
						pv.vars = c( 'math' , 'read' , 'scie' , 'scep' , 'sced' , 'scid' , 'skco' , 'skpe' , 'ssph' , 'ssli' , 'sses' ) ,
						implicates = 10 ,
						save_name = catalog[ i , 'design' ]
					)
								
				}
				
			}
			
			if( catalog[ i , 'year' ] == 2012 ){
			
					
				# download the file specified at the address constructed above,
				# then immediately import it into the monetdb server
				read_SAScii_monetdb ( 
					catalog[ i , 'full_url' ] ,
					sas_ri = pisa_remove_fakecnt_lines( pisa_find_chars( pisa_add_decimals( pisa_tcri( catalog[ i , 'sas_ri' ] ) , precise = TRUE ) ) ) , 
					zipped = TRUE ,
					tl = TRUE ,
					tablename = catalog[ i , 'db_tablename' ] ,
					skip_decimal_division = TRUE ,
					connection = db
				)
			
				# missing recodes #
			
				spss.script <- gsub( "\\.sas$" , ".txt" , gsub( "SAS" , "SPSS" , catalog[ i , 'sas_ri' ] ) )
			
				pisa_spss.based.missing.blankouts( db , catalog[ i , 'db_tablename' ] , spss.script )
			
				if( !is.na( catalog[ i , 'design' ] )  ){
				
					pisa_construct.pisa.survey.designs(
						db , 
						year = catalog[ i , 'year' ] ,
						table.name = catalog[ i , 'db_tablename' ] ,
						pv.vars = c( 'math' , 'macc' , 'macq' , 'macs' , 'macu' , 'mape' , 'mapf' , 'mapi' , 'read' , 'scie' ) ,
						save_name = catalog[ i , 'design' ]
					)
				
				}
				
			}
			
			
			if( catalog[ i , 'year' ] == 2009 ){
			
				read_SAScii_monetdb ( 
					catalog[ i , 'full_url' ] ,
					sas_ri = pisa_find_chars( pisa_add_decimals( pisa_remove_tabs( pisa_tcri( catalog[ i , 'sas_ri' ] ) ) ) ) , 
					zipped = TRUE ,
					tl = TRUE ,
					tablename = catalog[ i , 'db_tablename' ] ,
					skip_decimal_division = TRUE ,
					connection = db
				)

				
				# missing recodes #
	
				if( grepl( "int_stq09_dec11" , catalog[ i , 'full_url' ] , ignore.case = TRUE ) ) pisa_int_stq09_dec11.missings( db )
				
				if( grepl( "int_scq09_dec11" , catalog[ i , 'full_url' ] , ignore.case = TRUE ) ) pisa_int_scq09_dec11.missings( db )
				
				if( grepl( "int_par09_dec11" , catalog[ i , 'full_url' ] , ignore.case = TRUE ) ) {
					
					miss1.txt <- 
						"PA01Q01 PA01Q02 PA01Q03 PA02Q01 PA03Q01 PA03Q02 PA03Q03 PA03Q04 PA03Q05 PA03Q06 PA03Q07 PA03Q08 PA03Q09 PA04Q01 
						PA05Q01 PA06Q01 PA06Q02 PA06Q03 PA06Q04 PA07Q01 PA07Q02 PA07Q03 PA07Q04 PA07Q05 PA07Q06 PA08Q01 PA08Q02 PA08Q03 
						PA08Q04 PA08Q05 PA08Q06 PA08Q07 PA08Q08 PA09Q01 PA09Q02 PA09Q03 PA09Q04 PA10Q01 PA10Q02 PA10Q03 PA10Q04 PA11Q01 
						PA12Q01 PA13Q01 PA14Q01 PA14Q02 PA14Q03 PA14Q04 PA14Q05 PA14Q06 PA14Q07 PA15Q01 PA15Q02 PA15Q03 PA15Q04 PA15Q05 
						PA15Q06 PA15Q07 PA15Q08 PA16Q01 PA17Q01 PA17Q02 PA17Q03 PA17Q04 PA17Q05 PA17Q06 PA17Q07 PA17Q08 PA17Q09 PA17Q10 
						PA17Q11  PQMISCED PQFISCED PQHISCED"

					# in this table..these columns..with these values..should be converted to NA
					pisa_missing.updates( 
						db , 
						catalog[ i, 'db_tablename' ] , 
						pisa_split.n.clean( miss1.txt ) ,
						7:9 
					)

					# in this table..these columns..with these values..should be converted to NA
					pisa_missing.updates( 
						db , 
						catalog[ i, 'db_tablename' ] , 
						c( "PRESUPP" , "MOTREAD" , "READRES" , "CURSUPP" , "PQSCHOOL" , "PARINVOL" ) ,
						9997:9999 
					)
				
				}
				
				# note: no missing recodes for `int_cog09_s_dec11` or `int_cog09_td_dec11`
				
				# end of missing recodes #

				if( !is.na( catalog[ i , 'design' ] ) ){
				
					pisa_construct.pisa.survey.designs(
						db , 
						year = catalog[ i , 'year' ] ,
						table.name = catalog[ i , 'db_tablename' ] ,
						pv.vars = c( 'math' , 'read' , 'scie' , 'read1' , 'read2' , 'read3' , 'read4' , 'read5' ) ,
						save_name = catalog[ i , 'design' ]
					)
				
				}

			}


			if( catalog[ i , 'year' ] == 2006 ){
			
				read_SAScii_monetdb ( 
					catalog[ i , 'full_url' ] ,
					sas_ri = pisa_find_chars( pisa_add_decimals( pisa_remove_tabs( pisa_tcri( catalog[ i , 'sas_ri' ] ) ) ) ) , 
					zipped = TRUE ,
					tl = TRUE ,
					tablename = catalog[ i , 'db_tablename' ] ,
					skip_decimal_division = TRUE ,
					connection = db
				)
			
				# missing recodes #
		
				if( grepl( "int_stu06_dec07" , catalog[ i , 'full_url' ] , ignore.case = TRUE ) ) pisa_int_stu06_dec07.missings( db )
				
				if( grepl( "int_sch06_dec07" , catalog[ i , 'full_url' ] , ignore.case = TRUE ) ) pisa_int_sch06_dec07.missings( db )
				
				if( grepl( "int_par06_dec07" , catalog[ i , 'full_url' ] , ignore.case = TRUE ) ) pisa_int_par06_dec07.missings( db )
				
				if( grepl( "int_cogn06_t_dec07" , catalog[ i , 'full_url' ] , ignore.case = TRUE ) ) pisa_int_cogn06_t_dec07.missings( db )
				
				if( grepl( "int_cogn06_s_dec07" , catalog[ i , 'full_url' ] , ignore.case = TRUE ) ) pisa_int_cogn06_s_dec07.missings( db )
				
				# end of missing recodes #
				
				
				if( !is.na( catalog[ i , 'design' ] )  ){
					
					pisa_construct.pisa.survey.designs(
						db , 
						year = catalog[ i , 'year' ] ,
						table.name = catalog[ i , 'db_tablename' ] ,
						pv.vars = c( 'math' , 'read' , 'scie' , 'intr' , 'supp' , 'eps' , 'isi' , 'use' ) ,
						save_name = catalog[ i , 'design' ]
					)
					
				}
				
			}
			
			
			
			if( catalog[ i , 'year' ] == 2003 ){
			
				
				# get rid of some goofy `n` values in this ascii data
				if ( grepl( "INT_cogn_2003" , catalog[ i , 'full_url' ] ) ){
				
					zipped <- FALSE
					
					tf2 <- tempfile()
					
					cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

					unzipped_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , "/unzips" ) )

					# read-only file connection "r" - pointing to the ASCII file
					incon <- file( unzipped_files , "r")

					# write-only file connections "w"
					outcon <- file( tf2 , "w" )
					
					while( length( line <- readLines( incon , 10000 ) ) > 0 ){
						line <- gsub( "n" , " " , line , fixed = TRUE )
						writeLines( line , outcon )
					}

					close( outcon )
					close( incon , add = T )
					
					fp <- tf2
					
					# the sas importation script is screwey too.
					sri <- pisa_sas_is_evil( catalog[ i , 'sas_ri' ] )
					# fix it.
					
				} else {
					
					fp <- catalog[ i , 'full_url' ]
					sri <- catalog[ i , 'sas_ri' ]
					zipped <- TRUE
					
				}
			
				read_SAScii_monetdb ( 
					fp ,
					sas_ri = pisa_find_chars( pisa_add_decimals( pisa_remove_tabs( sri ) ) ) , 
					zipped = zipped ,
					tl = TRUE ,
					tablename = catalog[ i , 'db_tablename' ] ,
					skip_decimal_division = TRUE ,
					connection = db
				)
			
			
				# missing recodes #
				
				if( grepl( "int_cogn_2003" , catalog[ i , 'full_url' ] , ignore.case = TRUE ) ) pisa_missing.updates( db , catalog[ i , 'db_tablename' ]  , c( "CLCUSE3a" , "CLCUSE3b" ) , 997:999 )
				
				if( grepl( "int_stui_2003_v2" , catalog[ i , 'full_url' ] , ignore.case = TRUE ) ) pisa_int_stui_2003_v2.missings( db )
				
				if( grepl( "int_schi_2003" , catalog[ i , 'full_url' ] , ignore.case = TRUE ) ) pisa_int_schi_2003.missings( db )
				
				# end of missing recodes #
				
				
				if( !is.na( catalog[ i , 'design' ] )  ){

					pisa_construct.pisa.survey.designs(
						db , 
						year = catalog[ i , 'year' ] ,
						table.name = catalog[ i , 'db_tablename' ] ,
						pv.vars = c( 'math' , 'math1' , 'math2' , 'math3' , 'math4' , 'read' , 'scie' , 'prob' ) ,
						save_name = catalog[ i , 'design' ]
					)
				
				}
				
			}
			
			
			if( catalog[ i , 'year' ] == 2000 ){
					
				# well aren't you a pain in the ass as usual, mathematics?
				if ( grepl( "intstud_math" , catalog[ i , 'full_url' ] ) ) {
				
					# run some special cleanup functions to get the intstud_math sas script SAScii-compatible
					sri <- pisa_find_chars( pisa_add_decimals( pisa_add_sdt( pisa_remove_tabs( pisa_stupid_sas( catalog[ i , 'sas_ri' ] ) ) ) ) )
					
					# this one is annoying.
					# just read it into RAM (it fits under 4GB)
					# then save to MonetDB
					ism <- read_SAScii( catalog[ i , 'full_url' ] , sri , zipped = TRUE , na_values = c( "NA" , "." ) )
					
					# convert all column names to lowercase
					names( ism ) <- tolower( names( ism ) )
					
					# throw out the toss_0 column
					ism$toss_0 <- NULL
					
					# initiate a temporary file on the local disk
					tf <- tempfile()
					
					# write the `ism` data.frame object to the temporary file
					write.csv( ism , tf , row.names = FALSE )
					
					# read that csv file directly into monetdb
					DBI::dbWriteTable( db , catalog[ i , 'db_tablename' ] , tf , nrow.check = 20000 , na.strings = "NA" , lower.case.names = TRUE )

				} else {
			
					# clean up some of the sas import scripts
					sri <- pisa_find_chars( pisa_add_decimals( pisa_add_sdt( pisa_remove_tabs( catalog[ i , 'sas_ri' ] ) ) ) )
					
					# woah clean up even more.
					if ( grepl( "intstud_read|intstud_scie" , catalog[ i , 'full_url' ] , ignore.case = TRUE ) ) sri <- pisa_sas_is_quite_evil( sri )
					
					# download the file specified at the address constructed above,
					# then immediately import it into the monetdb server
					read_SAScii_monetdb ( 
						catalog[ i , 'full_url' ] ,
						sas_ri = sri , 
						zipped = TRUE ,
						tl = TRUE ,
						tablename = catalog[ i , 'db_tablename' ] ,
						skip_decimal_division = TRUE ,
						connection = db
					)
				
				}
				

				# missing recodes #
				
				# note: no missing recodes for `intcogn_v3`
				
				# intscho
				if( grepl( "intscho" , catalog[ i , 'full_url' ] , ignore.case = TRUE ) ) pisa_intscho.missings( db )

				# intstud_math
				if( grepl( "intstud_math" , catalog[ i , 'full_url' ] , ignore.case = TRUE ) ) pisa_intstud.missings( db , catalog[ i , 'db_tablename' ] )
					
				miss6.math <-
					c(
						"wlemath" , "wleread" , "wleread1" , "wleread2" , "wleread3" , "pv1math" , "pv2math" , "pv3math" , "pv4math" , "pv5math" , "pv1math1" , "pv2math1" , "pv3math1" , "pv4math1" , "pv5math1" , "pv1math2" , "pv2math2" , "pv3math2" , "pv4math2" , "pv5math2" , "pv1read" , "pv2read" , "pv3read" , "pv4read" , "pv5read" , "pv1read1" , "pv2read1" , "pv3read1" , "pv4read1" , "pv5read1" , "pv1read2" , "pv2read2" , "pv3read2" , "pv4read2" , "pv5read2" , "pv1read3" , "pv2read3" , "pv3read3" , "pv4read3" , "pv5read3" , "wlerr_m" , "wlerr_r" , "wlerr_r1" , "wlerr_r2" , "wlerr_r3"
					)
				
				# in this table..these columns..with these values..should be converted to NA
				if( grepl( "intstud_math" , catalog[ i , 'full_url' ] , ignore.case = TRUE ) ) pisa_missing.updates( db , catalog[ i , 'db_tablename' ] , miss6.math , 9997 )
				
				# intstud_read
				if( grepl( "intstud_read" , catalog[ i , 'full_url' ] , ignore.case = TRUE ) ) pisa_intstud.missings( db , catalog[ i , 'db_tablename' ] )
					
				miss6.read <-
					c(
						"wleread" , "wleread1" , "wleread2" , "wleread3" , "pv1read" , "pv2read" , "pv3read" , "pv4read" , "pv5read" , "pv1read1" , "pv2read1" , "pv3read1" , "pv4read1" , "pv5read1" , "pv1read2" , "pv2read2" , "pv3read2" , "pv4read2" , "pv5read2" , "pv1read3" , "pv2read3" , "pv3read3" , "pv4read3" , "pv5read3" , "wlerr_r" , "wlerr_r1" , "wlerr_r2" , "wlerr_r3"
					)
				
				# in this table..these columns..with these values..should be converted to NA
				if( grepl( "intstud_read" , catalog[ i , 'full_url' ] , ignore.case = TRUE ) ) pisa_missing.updates( db , catalog[ i , 'db_tablename' ]  , miss6.read , 9997 )
				
				# intstud_scie
				if( grepl( "intstud_scie" , catalog[ i , 'full_url' ] , ignore.case = TRUE ) ) pisa_intstud.missings( db , catalog[ i , 'db_tablename' ] )
				
				miss6.scie <-
					c( 
						"wleread" , "wleread1" , "wleread2" , "wleread3" , "wlescie" , "pv1read" , "pv2read" , "pv3read" , "pv4read" , "pv5read" , "pv1read1" , "pv2read1" , "pv3read1" , "pv4read1" , "pv5read1" , "pv1read2" , "pv2read2" , "pv3read2" , "pv4read2" , "pv5read2" , "pv1read3" , "pv2read3" , "pv3read3" , "pv4read3" , "pv5read3" , "pv1scie" , "pv2scie" , "pv3scie" , "pv4scie" , "pv5scie" , "wlerr_r" , "wlerr_r1" , "wlerr_r2" , "wlerr_r3" , "wlerr_s"
					)
				
				# in this table..these columns..with these values..should be converted to NA
				if( grepl( "intstud_scie" , catalog[ i , 'full_url' ] , ignore.case = TRUE ) ) pisa_missing.updates( db , catalog[ i , 'db_tablename' ]  , miss6.scie , 9997 )
				
				# end of missing recodes #
				
				
				if( !is.na( catalog[ i , 'design' ] )  ){
				
					if( grepl( "intstud_math" , catalog[ i , 'full_url' ] , ignore.case = TRUE ) ){
					
						# use the table (already imported into monetdb) to spawn five different tables (one for each plausible [imputed] value)
						# then construct a multiply-imputed, monetdb-backed, replicated-weighted complex-sample survey-design object-object.
						pisa_construct.pisa.survey.designs(
							db , 
							year = catalog[ i , 'year' ] ,
							table.name = catalog[ i , 'db_tablename' ] ,
							pv.vars = c( 'math' , 'math1' , 'math2' , 'read' , 'read1' , 'read2' , 'read3' ) ,
							save_name = catalog[ i , 'design' ]
						)

					}
					
					if( grepl( "intstud_read" , catalog[ i , 'full_url' ] , ignore.case = TRUE ) ){
					
						# use the table (already imported into monetdb) to spawn five different tables (one for each plausible [imputed] value)
						# then construct a multiply-imputed, monetdb-backed, replicated-weighted complex-sample survey-design object-object.	
						pisa_construct.pisa.survey.designs(
							db , 
							year = catalog[ i , 'year' ] ,
							table.name = catalog[ i , 'db_tablename' ] ,
							pv.vars = c( 'read' , 'read1' , 'read2' , 'read3' ) ,
							save_name = catalog[ i , 'design' ]
						)
						
					}
					
					if( grepl( "intstud_scie" , catalog[ i , 'full_url' ] , ignore.case = TRUE ) ){
					
						# use the table (already imported into monetdb) to spawn five different tables (one for each plausible [imputed] value)
						# then construct a multiply-imputed, monetdb-backed, replicated-weighted complex-sample survey-design object-object.
						pisa_construct.pisa.survey.designs(
							db , 
							year = catalog[ i , 'year' ] ,
							table.name = catalog[ i , 'db_tablename' ] ,
							pv.vars = c( 'read' , 'read1' , 'read2' , 'read3' , 'scie' ) ,
							save_name = catalog[ i , 'design' ]
						)
					
					}
					
				}



			}
			
			tables_after <- setdiff( tables_before , DBI::dbListTables( db ) )
			
			for( this_table in tables_after ) catalog[ i , 'case_count' ] <- max( catalog[ i , 'case_count' ] , DBI::dbGetQuery( db , paste0( "SELECT COUNT(*) FROM " , this_table ) )[ 1 , 1 ] , na.rm = TRUE )
			
			# delete the temporary files
			suppressWarnings( file.remove( tf , if( exists( "unzipped_files" ) ) unzipped_files ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'dbfile' ] , "'\r\n\n" ) )

		}

		on.exit()
		
		catalog

	}


	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
# # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # #
# # functions related to downloads and imporation # #
# # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # #

# mac / unix does not like standard read-ins, so use text connection read-ins
pisa_tcri <-
	function( sas_ri ){
		tf <- tempfile()
		
		ao <- try({
			z <- textConnection( RCurl::getURL( sas_ri ) )
			writeLines( z , tf ) } , silent = TRUE )
		
		if( class( ao ) == 'try-error' ){
			z <- RCurl::getURL( sas_ri )
			writeLines( z , tf )
		}
		
		tf
	}
	
# some of the year 2000 pisa files do not start at column position 1
# which SAScii cannot handle, so manually add a single-digit toss
pisa_add_sdt <-
	function( sas_ri ){
		tf <- tempfile()
		z <- readLines( sas_ri )
		z <- gsub( "country " , "toss_0 $ 1-1 country " , z , fixed = TRUE )
		z <- gsub( "cnt                  1555-1557" , "cnt $ 1555-1557" , z , fixed = TRUE )
		writeLines( z , tf )
		tf
	}
		

# stupid recodes for one stupid sas importation file stupid sas
# http://pisa2000.acer.edu.au/downloads/intstud_math.sas
pisa_stupid_sas <-
	function( sas_ri ){
		tf <- tempfile()
		z <- readLines( sas_ri )
		
		z[ 1252:1261 ] <-
			c( 
				"pv1math1 1568-1574" , 
				"pv2math1 1575-1581" , 
				"pv3math1 1582-1588" , 
				"pv4math1 1589-1595" , 
				"pv5math1 1596-1602" , 
				"pv1math2 1603-1609" , 
				"pv2math2 1610-1616" , 
				"pv3math2 1617-1623" , 
				"pv4math2 1624-1630" , 
				"pv5math2 1631-1637" 
			)
		
		writeLines( z , tf )
		
		tf
	}
	

# another sas import script is uppercase one place 
# and lowercase elsewhere, and it's cramping my style
pisa_sas_is_evil <-
	function( sas_ri ){
		
		tf <- tempfile()
		z <- readLines( sas_ri )
		
		z <- gsub( "S304Q03a" , "S304Q03A" , z )
		z <- gsub( "S304Q03b" , "S304Q03B" , z )
		
		writeLines( z , tf )
		
		tf
	}

pisa_remove_fakecnt_lines <-
	function( sas_ri ){
		
		tf <- tempfile()
		z <- readLines( sas_ri )
		
		z <- z[ !grepl( "fakecnt" , tolower( z ) ) ]
	
		writeLines( z , tf )
		
		tf
	}

# yet another silly specific hardcoded recode
# because sas is a trainwreck of a language
pisa_sas_is_quite_evil <-
	function( sas_ri ){
		
		tf <- tempfile()
		z <- readLines( sas_ri )
		
		z <- gsub( "cnt   1555-1557" , "cnt $  1555-1557" , z )
	
		writeLines( z , tf )
		
		tf
	}

		
# remove tabs and other illegal things
pisa_remove_tabs <-
	function( sas_ri ){
		tf <- tempfile()
		z <- readLines( sas_ri )
		z <- gsub( "\t" , " " , z )
		z <- gsub( "SELECT" , "SELECT_" , z , fixed = TRUE )
		z <- gsub( "@1559 (read_waf) (1*7.5)" , "read_waf 1559-1565" , z , fixed = TRUE )
		z <- gsub( "VER_COGN   506-519;" , "VER_COGN   506-518;" , z , fixed = TRUE )

		writeLines( z , tf )
		tf
	}



pisa_add_decimals <-
	function( sas_ri , precise = FALSE ){
	
		tf <- tempfile()
		
		z <- readLines( sas_ri )
		
		z <- stringr::str_trim( z )

		# find strings that end with number dot number #

		# lines needing decimals
		lnd <- strsplit( z[ grep( "*[1-9]\\.[1-9]$" , z ) ] , " " )

		# if there aren't any matches, this function has no purpose.
		if ( length( lnd ) == 0 ) return( sas_ri )
		
		# remove blanks
		lnd <- lapply( lnd , function( z ) z[ z != "" ] )

		# variables needing decimals
		vnd <- unlist( lapply( lnd , "[[" , 1 ) )

		# variables need a following space otherwise the match does not work
		vnd <- paste0( vnd , " " )
		
		# decimals to paste
		dtp <- unlist( lapply( lnd , "[[" , 2 ) )

		# if the precision flag is marked..
		if ( precise ){
		
			# loop through every variable needing decimals
			for ( i in seq( length( vnd ) ) ){

				# search for strings beginning with the *exact* string
				begins.with.length <- nchar( vnd[ i ] )
				
				lines.to.replace <- substr( z , 1 , begins.with.length ) == vnd[ i ]
		
				z[ lines.to.replace ] <- paste( z[ lines.to.replace ] , dtp[ i ] )
		
			}
		
		} else {
		
			# loop through every variable needing decimals
			# and replace the variable text with the variable plus the number dot number
			for ( i in seq( length( vnd ) ) ) z[ grep( vnd[ i ] , z ) ] <- paste( z[ grep( vnd[ i ] , z ) ] , dtp[ i ] )
			
		}
			
		writeLines( z , tf )
		
		tf
	}


	
pisa_find_chars <-
	function( sas_ri ){
		
		# test if this is necessary
		suppressWarnings( z <- SAScii::parse.SAScii( sas_ri ) )
		
		# if there are ZERO character fields (that's not possible)
		# they need to be pulled from the "length" segment
		if ( any( z$char , na.rm = TRUE ) ){
		
			# so if that's not the case,
			# just stop right here.
			return( sas_ri )
			
		} else {

			# create a temporary file
			tf <- tempfile()
		
			# take the original input file..
			
			# read it in
			z <- readLines( sas_ri )
					
			# find the word `length` and replace it..
			z <- gsub( "length" , "input" , z )
			
			# also spread out the $s
			z <- gsub( "$" , " $ " , z , fixed = TRUE )
			
			# then write it back to the temporary file..
			writeLines( z , tf )
			
			# for a special-read in of those fields
			suppressWarnings( z <- SAScii::parse.SAScii( tf ) )
			

			# but just do that to take note of the character fields
			char.fields <- z[ z$char , 'varname' ]
			
			# now read it in for real
			z <- readLines( sas_ri )
			
			# and add dollar signs to the appropriate lines..
			# (note - add a space in the search to assure right-hand-side whole-word-only
			for ( i in char.fields ) z <- gsub( paste0( i , " \\$*" ) , paste( i , "$" ) , z )
			
			# ..then write them to the temporary file
			writeLines( z , tf )

			# and return the result of that dollar sign pull
			return( tf )
		} 

	}
	

	
	
# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # functions related to survey statistical analysis  # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # #


# combine five sqlrepsurvey designs
# into a single monetdb-backed multiply-imputed replicate-weighted list
svyMDBdesign <-
	function( my_design ){
	
		# open each of those design connections with MonetDB hooray
		my_design$designs <- lapply( my_design$designs , open , RSQLite::SQLite() )

		class( my_design ) <- 'svyMDBimputationList'
		
		my_design
	}


# need to copy over the `with` method
with.svyMDBimputationList <- survey:::with.svyimputationList
	
update.svyMDBimputationList <-
	function( my_design , ... ){
	
		z <- my_design
	
		z$designs <- lapply( my_design$designs , update , ... )
	
		z$call <- sys.call(-1)
		
		z
	}


# and create a new subset method for MDB imputation lists.
subset.svyMDBimputationList <-
	function( x , ... ){
		
		z <- x
		
		z$designs <- lapply( x$designs , subset , ... )
		
		z$call <- sys.call(-1)
		
		z
	}
# thanks.
# http://stackoverflow.com/questions/17407852/how-to-pass-an-expression-through-a-function-for-the-subset-function-to-evaluate


# initiate a pisa-specific survey design-adjusted t-test
# that will work on monetdb-backed, multiply-imputed designs
pisa.svyttest <-
	function( formula , design ){

		# the MIcombine function runs differently than a normal svyglm() call
		m <- eval(bquote(mitools::MIcombine( with( design , survey::svyglm(formula))) ) )

		rval <-
			list(
				statistic = coef( m )[ 1 ] / survey::SE( m )[ 1 ] ,
				parameter = m$df[ 1 ] ,		
				estimate = coef( m )[ 1 ] ,
				null.value = 0 ,
				alternative = "two.sided" ,
				method = "Design-based t-test" ,
				data.name = deparse( formula ) 
			)
				   
		rval$p.value <- 
			( 1 - pf( ( rval$statistic )^2 , 1 , m$df[ 1 ] ) )

		names( rval$statistic ) <- "t"
		names( rval$parameter ) <- "df"
		names( rval$estimate ) <- "difference in mean"
		names( rval$null.value ) <- "difference in mean"
		class( rval ) <- "htest"

		return(rval)
	}



# # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # #
# # functions related to survey design creation   # #
# # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # #


pisa_construct.pisa.survey.designs <-
	function( conn , year , table.name , pv.vars , implicates = 5 , save_name ){

		# identify all variables that are multiply-imputed
		pv.colnames <- paste0( "pv" , outer( seq( implicates ) , pv.vars , paste0 ) )

		# identify all variables that are *not* multiply-imputed
		table.fields <- DBI::dbListFields( conn , table.name )

		nmi <- table.fields[ !( table.fields %in% pv.colnames ) ]

		# 'read' is not a valid column name in monetdb.
		nr.pv.vars <- gsub( "read" , "readZ" , pv.vars )

		# loop through each of the five variables..
		for ( i in seq( implicates ) ){
		
			implicate.name <- paste0( table.name , "_imp" , i )
			
			# build a sql string to create all five implicates
			sql <-
				paste(

					paste0( "create table " , implicate.name ) ,
					
					"as select" ,
					
					# all non-multiply imputed values
					paste( nmi , collapse = ", " ) ,
					
					# one of the five multiply-imputed values,
					# using a sql AS clause to rename
					paste0( ", pv" , i , pv.vars , " as " , nr.pv.vars , sep = "" , collapse = "" ) ,
					
					"from" ,
					
					table.name ,
					
					"with data"
				)
			

			# actually create the current implicate,
			# using the string constructed above
			DBI::dbSendQuery( conn , sql )

			
			# add an empty column called `one` that's an integer
			DBI::dbSendQuery( 
				conn , 
				paste( 
					'alter table' ,
					implicate.name ,
					'add column one int' 
				)
			)
			
			
			# fill it full of ones
			DBI::dbSendQuery( 
				conn , 
				paste( 
					'UPDATE' ,
					implicate.name ,
					'SET one = 1' 
				)
			)
			
			
		}

		
		# construct the actual monetdb-backed,
		# replicate-weighted survey design.
		this_design <-
			survey::svrepdesign( 	
				weights = ~w_fstuwt , 
				repweights = if( year >= 2015 ) "w_fsturwt[1-9]" else "w_fstr[1-9]" , 
				scale = 4 / 80 ,
				rscales = rep( 1 , 80 ) ,
				mse = TRUE ,
				type = 'JK1' ,
				data = mitools::imputationList( datasets = as.list( paste0( table.name , "_imp" , seq( implicates ) ) ) , dbtype = "SQLite" ) ,
				dbtype = "SQLite" ,
				dbname = DBI::dbGetInfo( conn )$gdk_dbpath
			)
		
		# workaround for a bug in survey::svrepdesign.character
		this_design$mse <- TRUE
		
		# save all of the database design objects as r data files
		saveRDS( this_design , file = save_name , compress = FALSE )

		save_name
	}

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
# pull the missing values from the spss importation script
# and loop through the table within the monetdb,
# blanking out those values.
pisa_spss.based.missing.blankouts <-
	function(
		conn ,
		tablename ,
		spss_script
	){
	
		# load the spss script directly into working memory
		z <- readLines( spss_script )

		# limit the lines to those indicating missing values
		z <- z[ grep( 'Missing values' , z ) ]

		# remove that text
		z <- gsub( 'Missing values ' , '' , z )

		# remove the final period
		z <- gsub( '.' , '' , z , fixed = TRUE )

		# break the line at the space
		split <- strsplit( z , ' ' )

		# capture all variable names into one object..
		vars <- unlist( lapply( split , '[[' , 1 ) )
		
		# ..and all values to be blanked out in a second
		vals <- unlist( lapply( split , '[[' , 2 ) )

		# loop through all variables that have any official missing values
		for ( i in seq( length( vars ) ) ){

			# write out the UPDATE line that will set certain values to null
			update.sql <-
				paste(
					"UPDATE" , 
					tablename ,
					"SET" , 
					vars[ i ] , 
					"= NULL WHERE CAST( " ,
					vars[ i ] ,
					" AS DOUBLE ) IN" ,
					vals[ i ]
				)
			
			# execute the update line
			perhaps.uncast <- try( DBI::dbSendQuery( conn , update.sql ) , silent = TRUE )
			
			# but if that line failed..
			if( class( perhaps.uncast ) == 'try-error' ){
				
				# then re-try it without the casting to double
				
				# write out the UPDATE line that will set certain values to null
				update.sql <-
					paste(
						"UPDATE" , 
						tablename ,
						"SET" , 
						vars[ i ] , 
						"= NULL WHERE" ,
						vars[ i ] ,
						"IN" ,
						vals[ i ]
					)
				
				# re-execute the query hooray
				DBI::dbSendQuery( conn , update.sql )
				
				
			}
			
		}

		TRUE
	}



# take a character string separated by spaces,
# remove all return characters (\n)
# and remove all empty strings
# then return the resultant character vector
pisa_split.n.clean <-
	function( txt ){
		x <- strsplit( txt , " " )[[1]]
		x <- gsub( "\n" , "" , x )
		x <- x[ x != "" ]
		x
	}
	
# take a monetdb connection,
# a table's name within the monet database
# a character string full of variables
# and a numeric vector to search for -
# in order to replace each of these values with NULL
pisa_missing.updates <-
	function( 
		db , 
		tablename ,
		missing.variables ,
		missing.values
	){
		
		# loop through the character vector
		for ( i in missing.variables ){
		
			# wherever the current variable `i` is one of the possible
			# `missing.values` (passed in at the start of this function)
			# replace it with NULL
			DBI::dbSendQuery( 
				db , 
				paste(
					'UPDATE' ,
					tablename ,
					'SET' ,
					i ,
					" = NULL WHERE CAST( " ,
					i ,
					" AS DOUBLE ) IN (" ,
					paste( missing.values , collapse = " , " ) ,
					")"
				)
			)
		}
	
		TRUE
	}


# # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # #
# # the remaining functions in this script are    # #
# # used for overwriting specific tables, using   # #
# # all the helper functions constructed above    # #
# # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # #

pisa_intscho.missings <- 
	function( db ){

		miss1 <-
			c(
				"SC01Q01" , "SC03Q01" , "SC05Q01" , "SC05Q02" , "SC05Q03" , "SC05Q04" , "SC05Q05" , "SC05Q06" , "SC05Q07" , "SC05Q08" , "SC05Q09" , "SC05Q10" , "SC05Q11" , "SC05Q12" , "SC05Q13" , "SC05Q14" , "SC07Q01" , "SC07Q02" , "SC07Q03" , "SC07Q04" , "SC07Q05" , "SC07Q06" , "SC07Q07" , "SC09Q01" , "SC09Q02" , "SC09Q03" , "SC09Q04" , "SC09Q05" , "SC10Q01" , "SC10Q02" , "SC10Q03" , "SC10Q04" , "SC10Q05" , "SC10Q06" , "SC11Q01" , "SC11Q02" , "SC11Q03" , "SC11Q04" , "SC11Q05" , "SC11Q06" , "SC11Q07" , "SC11Q08" , "SC11Q09" , "SC12Q01" , "SC12Q02" , "SC12Q03" , "SC12Q04" , "SC12Q05" , "SC16Q01" , "SC16Q02" , "SC16Q03" , "SC16Q04" , "SC16Q05" , "SC17Q01" , "SC17Q02" , "SC17Q03" , "SC18Q01" , "SC18Q02" , "SC18Q03" , "SC18Q04" , "SC18Q05" , "SC18Q06" , "SC19Q01" , "SC19Q02" , "SC19Q03" , "SC19Q04" , "SC19Q05" , "SC19Q06" , "SC19Q07" , "SC19Q08" , "SC19Q09" , "SC19Q10" , "SC19Q11" , "SC19Q12" , "SC19Q13" , "SC19Q14" , "SC19Q15" , "SC19Q16" , "SC19Q17" , "SC20Q01" , "SC20Q02" , "SC20Q03" , "SC20Q04" , "SC21Q01" , "SC21Q02" , "SC21Q03" , "SC21Q04" , "SC21Q05" , "schltype" , "pcgirls" , "percomp1" , "percomp2" , "percomp3" , "percomp4" , "percomp5" , "propqual" , "propcert" , "propread" , "propmath" , "propscie"
			)

		pisa_missing.updates( db , 'intscho' , miss1 , 7:9 )

		miss2 <- c( "SC02Q01" , "SC02Q02" , "SC13Q01" , "SC13Q02" , "SC13Q03" , "SC13Q04" , "SC13Q05" , "SC13Q06" , "tothrs" )

		pisa_missing.updates( db , 'intscho' , miss2 , c( 9 , 9997:9999 ) )
		
		miss3 <-
			c( 
				"SC04Q01" , "SC04Q02" , "SC04Q03" , "SC04Q04" , "SC06Q03" , "SC08Q01" , "SC08Q02" , "SC08Q03" , "SC08Q04" , "SC08Q05" , "SC08Q06" , "SC14Q01" , "SC14Q02" , "SC14Q03" , "SC14Q04" , "SC14Q05" , "SC14Q06" , "SC14Q07" , "SC14Q08" , "SC14Q09" , "SC14Q10" , "SC14Q11" , "SC14Q12" , "SC14Q13" , "SC14Q14" , "SC14Q15" , "SC14Q16" , "SC14Q17" , "SC14Q18" , "SC15Q01"
			)

		pisa_missing.updates( db , 'intscho' , miss3 , 997:999 )
			
		
		miss4 <- c( "SC06Q01" , "SC06Q02" , "stratio" , "scmatedu" , "tcshort" , "teacbeha" , "studbeha" , "tcmorale" , "schauton" , "tchparti" , "scmatbui" , "ratcomp" )
		
		pisa_missing.updates( db , 'intscho' , miss4 , 97:99 )
		
		pisa_missing.updates( db , 'intscho' , 'schlsize' , 99997:99999 )
	
	}
	

pisa_intstud.missings <- 
	function( db , tablename ){

		miss1 <-
			c( 
				"ST03Q01" , "ST04Q01" , "ST04Q02" , "ST04Q03" , "ST04Q04" , "ST04Q05" , "ST04Q06" , "ST04Q07" , "ST04Q08" , "ST05Q01" , "ST05Q02" , "ST05Q03" , "ST06Q01" , "ST07Q01" , "ST12Q01" , "ST13Q01" , "ST14Q01" , "ST15Q01" , "ST16Q01" , "ST16Q02" , "ST16Q03" , "ST17Q01" , "ST18Q01" , "ST18Q02" , "ST18Q03" , "ST18Q04" , "ST18Q05" , "ST18Q06" , "ST19Q01" , "ST19Q02" , "ST19Q03" , "ST19Q04" , "ST19Q05" , "ST19Q06" , "ST20Q01" , "ST20Q02" , "ST20Q03" , "ST20Q04" , "ST20Q05" , "ST20Q06" , "ST21Q01" , "ST21Q02" , "ST21Q03" , "ST21Q04" , "ST21Q05" , "ST21Q06" , "ST21Q07" , "ST21Q08" , "ST21Q09" , "ST21Q10" , "ST21Q11" , "ST22Q01" , "ST22Q02" , "ST22Q03" , "ST22Q04" , "ST22Q05" , "ST22Q06" , "ST22Q07" , "ST23Q01" , "ST23Q02" , "ST23Q03" , "ST23Q04" , "ST24Q01" , "ST24Q02" , "ST24Q03" , "ST24Q04" , "ST24Q05" , "ST24Q06" , "ST24Q07" , "ST25Q01" , "ST26Q01" , "ST26Q02" , "ST26Q03" , "ST26Q04" , "ST26Q05" , "ST26Q06" , "ST26Q07" , "ST26Q08" , "ST26Q09" , "ST26Q10" , "ST26Q11" , "ST26Q12" , "ST26Q13" , "ST26Q14" , "ST26Q15" , "ST26Q16" , "ST26Q17" , "ST27Q02" , "ST27Q04" , "ST27Q06" , "ST29Q01" , "ST29Q02" , "ST29Q03" , "ST30Q01" , "ST30Q02" , "ST30Q03" , "ST30Q04" , "ST30Q05" , "ST31Q01" , "ST31Q02" , "ST31Q03" , "ST31Q04" , "ST31Q05" , "ST31Q06" , "ST31Q07" , "ST31Q08" , "ST32Q01" , "ST32Q02" , "ST32Q03" , "ST32Q04" , "ST32Q05" , "ST32Q06" , "ST32Q07" , "ST33Q01" , "ST33Q02" , "ST33Q03" , "ST34Q01" , "ST35Q01" , "ST35Q02" , "ST35Q03" , "ST35Q04" , "ST35Q05" , "ST35Q06" , "ST35Q07" , "ST35Q08" , "ST35Q09" , "ST36Q01" , "ST36Q02" , "ST36Q03" , "ST36Q04" , "ST36Q05" , "ST36Q06" , "ST38Q01" , "ST39Q01" , "ST39Q02" , "ST39Q03" , "ST39Q04" , "ST39Q05" , "ST41Q04" , "ST41Q05" , "ST41Q06" , "ST41Q07" , "ST41Q08" , "ST41Q09" , "IT01Q01" , "IT01Q02" , "IT01Q03" , "IT01Q04" , "IT02Q01" , "IT02Q02" , "IT02Q03" , "IT03Q01" , "IT04Q01" , "IT04Q02" , "IT04Q03" , "IT04Q04" , "IT05Q01" , "IT05Q02" , "IT05Q03" , "IT05Q04" , "IT06Q01" , "IT06Q02" , "IT06Q03" , "IT06Q04" , "IT06Q05" , "IT07Q01" , "IT08Q01" , "IT09Q01" , "IT10Q01" , "CC01Q01" , "CC01Q02" , "CC01Q03" , "CC01Q04" , "CC01Q05" , "CC01Q06" , "CC01Q07" , "CC01Q08" , "CC01Q09" , "CC01Q10" , "CC01Q11" , "CC01Q12" , "CC01Q13" , "CC01Q14" , "CC01Q15" , "CC01Q16" , "CC01Q17" , "CC01Q18" , "CC01Q19" , "CC01Q20" , "CC01Q21" , "CC01Q22" , "CC01Q23" , "CC01Q24" , "CC01Q25" , "CC01Q26" , "CC01Q27" , "CC01Q28" , "CC02Q01" , "CC02Q02" , "CC02Q03" , "CC02Q04" , "CC02Q05" , "CC02Q06" , "CC02Q07" , "CC02Q08" , "CC02Q09" , "CC02Q10" , "CC02Q11" , "CC02Q12" , "CC02Q13" , "CC02Q14" , "CC02Q15" , "CC02Q16" , "CC02Q17" , "CC02Q18" , "CC02Q19" , "CC02Q20" , "CC02Q21" , "CC02Q22" , "CC02Q23" , "CC02Q24" , "famstruc" , "brthord" , "fisced" , "misced"
			)

		pisa_missing.updates( db , tablename , miss1 , 7:9 )

		miss2 <- 
			c( "ST02Q01" , "ST27Q01" , "ST27Q03" , "ST27Q05" , "ST28Q01" , "ST28Q02" , "ST28Q03" , "ST37Q01" , "BMMJ" , "BFMJ" , "BTHR" , "nsib" , "isei" , "hisei" )

		pisa_missing.updates( db , tablename , miss1 , 97:99 )

		miss3 <- c( "ST41Q01" , "ST41Q02" , "ST41Q03" , "age" )

		pisa_missing.updates( db , tablename , miss3 , 997:999 )

		miss4 <-
			c( 
				"cultcom" , "soccom" , "famedsup" , "wealth" , "hedres" , "cultactv" , "cultposs" , "hmwktime" , "teachsup" , "disclima" , "studrel" , "achpress" , "belong" , "joyread" , "divread" , "comab" , "comuse" , "comatt" , "cstrat" , "effper" , "memor" , " selfef" , "cexp" , "elab" , "insmot" , "insmat" , "matcon" , "intrea" , "scacad" , "scverb" , "comlrn" , "coplrn"
			)	
		
		pisa_missing.updates( db , tablename , miss4 , 97 )
		
		pisa_missing.updates( db , tablename , c( "rmins" , "mmins" , "smins" ) , 9997:9999 )
		
	}
	
	
pisa_int_stu06_dec07.missings <-
	function( db ){
						
		miss9.txt <-
			"ST11Q01 ST11Q02 ST11Q03 ST04Q01 ST06Q01 ST07Q01 ST07Q02 ST07Q03 ST09Q01 ST10Q01 ST10Q02 ST10Q03 ST12Q01 ST13Q01 ST13Q02 ST13Q03 ST13Q04 ST13Q05 ST13Q06 ST13Q07 ST13Q08 ST13Q09 ST13Q10 ST13Q11 ST13Q12 ST13Q13 ST13Q14 ST14Q01 ST14Q02 ST14Q03 ST14Q04  ST15Q01 ST16Q01 ST16Q02 ST16Q03 ST16Q04 ST16Q05 ST17Q01 ST17Q02 ST17Q03 ST17Q04 ST17Q05 ST17Q06 ST17Q07 ST17Q08 ST18Q01 ST18Q02 ST18Q03 ST18Q04 ST18Q05 ST18Q06 ST18Q07 ST18Q08 ST18Q09 ST18Q10 ST19Q01 ST19Q02 ST19Q03 ST19Q04 ST19Q05 ST19Q06 ST20QA1 ST20QA2 ST20QA3 ST20QA4 ST20QA5 ST20QA6 ST20QB1 ST20QB2 ST20QB3 ST20QB4 ST20QB5 ST20QB6 ST20QC1 ST20QC2 ST20QC3 ST20QC4 ST20QC5 ST20QC6 ST20QD1 ST20QD2 ST20QD3 ST20QD4 ST20QD5 ST20QD6 ST20QE1 ST20QE2 ST20QE3 ST20QE4 ST20QE5 ST20QE6 ST20QF1 ST20QF2 ST20QF3 ST20QF4 ST20QF5 ST20QF6 ST20QG1 ST20QG2 ST20QG3 ST20QG4 ST20QG5 ST20QG6 ST20QH1 ST20QH2 ST20QH3 ST20QH4 ST20QH5 ST20QH6 ST21Q01 ST21Q02 ST21Q03 ST21Q04 ST21Q05 ST21Q06 ST21Q07 ST21Q08 ST22Q01 ST22Q02 ST22Q03 ST22Q04 ST22Q05 ST23QA1 ST23QA2 ST23QA3 ST23QA4 ST23QA5 ST23QA6 ST23QB1 ST23QB2 ST23QB3 ST23QB4 ST23QB5 ST23QB6 ST23QC1 ST23QC2 ST23QC3 ST23QC4 ST23QC5 ST23QC6 ST23QD1 ST23QD2 ST23QD3 ST23QD4 ST23QD5 ST23QD6 ST23QE1 ST23QE2 ST23QE3 ST23QE4 ST23QE5 ST23QE6 ST23QF1 ST23QF2 ST23QF3 ST23QF4 ST23QF5 ST23QF6 ST24Q01 ST24Q02 ST24Q03 ST24Q04 ST24Q05 ST24Q06 ST25Q01 ST25Q02 ST25Q03 ST25Q04 ST25Q05 ST25Q06 ST26Q01 ST26Q02 ST26Q03 ST26Q04 ST26Q05 ST26Q06 ST26Q07 ST27Q01 ST27Q02 ST27Q03 ST27Q04 ST28Q01 ST28Q02 ST28Q03 ST28Q04 ST29Q01 ST29Q02 ST29Q03 ST29Q04 ST31Q01 ST31Q02 ST31Q03 ST31Q04 ST31Q05 ST31Q06 ST31Q07 ST31Q08 ST31Q09 ST31Q10 ST31Q11 ST31Q12 ST32Q01 ST32Q02 ST32Q03 ST32Q04 ST32Q05 ST32Q06 ST33Q11 ST33Q12 ST33Q21 ST33Q22 ST33Q31 ST33Q32 ST33Q41 ST33Q42 ST33Q51 ST33Q52 ST33Q61 ST33Q62 ST33Q71 ST33Q72 ST33Q81 ST33Q82 ST34Q01 ST34Q02 ST34Q03 ST34Q04 ST34Q05 ST34Q06 ST34Q07 ST34Q08 ST34Q09 ST34Q10 ST34Q11 ST34Q12 ST34Q13 ST34Q14 ST34Q15 ST34Q16 ST34Q17 ST35Q01 ST35Q02 ST35Q03 ST35Q04 ST35Q05 ST36Q01 ST36Q02 ST36Q03 ST37Q01 ST37Q02 ST37Q03 ST37Q04 ST37Q05 ST37Q06 IC01Q01 IC02Q01 IC03Q01 IC03Q02 IC03Q03 IC04Q01 IC04Q02 IC04Q03 IC04Q04 IC04Q05 IC04Q06 IC04Q07 IC04Q08 IC04Q09 IC04Q10 IC04Q11 IC05Q01 IC05Q02 IC05Q03 IC05Q04 IC05Q05 IC05Q06 IC05Q07 IC05Q08 IC05Q09 IC05Q10 IC05Q11 IC05Q12 IC05Q13 IC05Q14 IC05Q15 IC05Q16"

		miss9 <- pisa_split.n.clean( miss9.txt )
		
		pisa_missing.updates( db , 'INT_Stu06_Dec07' , miss9 , 7:9 )
		
		pisa_missing.updates( db , 'INT_Stu06_Dec07' , c( "ST01Q01" , "ST02Q01" ) , c( 96 , 99 ) )
		
		miss5.txt <- "ISCEDL ISCEDD ISCEDO MISCED FISCED HISCED IMMIG MsECATEG FsECATEG HsECATEG SRC_M SRC_F SRC_E SRC_S"
		miss5 <- pisa_split.n.clean( miss5.txt )
		pisa_missing.updates( db , 'INT_Stu06_Dec07' , miss5 , 7:9 )
		
		pisa_missing.updates( db , 'INT_Stu06_Dec07' , c( "AGE" , "BMMJ" , "BFMJ" , "BSMJ" , "HISEI" , "PARED" , "ST11Q04" ) , c( 7 , 97:99 ) )
		
		
		miss7.txt <-
			"CLCUSE3A CLCUSE3B DEFFORT ESCS CARINFO CARPREP CULTPOSS ENVAWARE ENVOPT ENVPERC GENSCIE                    
			HEDRES HIGHCONF HOMEPOS INSTSCIE INTCONF INTSCIE INTUSE JOYSCIE             
			PERSCIE PRGUSE  RESPDEV SCAPPLY SCHANDS SCIEACT SCIEEFF SCIEFUT             
			SCINTACT SCINVEST SCSCIE WEALTH"
			
		pisa_missing.updates( 
			db , 
			'INT_Stu06_Dec07' , 
			pisa_split.n.clean( miss7.txt ) ,
			997:999 
		)
		
		pisa_missing.updates( 
			db , 
			'INT_Stu06_Dec07' , 
			paste0( 'pv' , 1:5 , 'read' ) ,
			9997 
		)
	}


pisa_int_sch06_dec07.missings <-
	function( db ){

		miss1.txt <-
			"SC02Q01 SC04Q01 SC04Q02 SC04Q03 SC04Q04 SC04Q05 SC04Q06 SC04Q07 SC04Q08 SC04Q09 SC04Q10 SC04Q11 SC04Q12  
			SC04Q13 SC04Q14 SC07Q01 SC08Q01 SC08Q02 SC10Q01 SC14Q01 SC14Q02 SC14Q03 SC14Q04 SC14Q05 SC14Q06 SC14Q07  
			SC14Q08 SC14Q09 SC14Q10 SC14Q11 SC14Q12 SC14Q13 SC15Q01 SC15Q02 SC15Q03 SC16Q01 SC17Q01 SC17Q02 SC17Q03  
			SC17Q04 SC17Q05 SC18Q01 SC19Q01 SC19Q02 SC19Q03 SC19Q04 SC19Q05 SC19Q06 SC20Q01 SC20Q02 SC20Q03 SC20Q04  
			SC20Q05 SC21Q01 SC21Q02 SC21Q03 SC21Q04 SC22Q01 SC22Q02 SC22Q03 SC22Q04 SC22Q05 SC23Q01 SC23Q02 SC23Q03  
			SC24Q01 SC25Q01 SC26Q01 SC27Q01 SC28Q01 SC29Q01"

		pisa_missing.updates( 
			db , 
			'INT_Sch06_Dec07' , 
			pisa_split.n.clean( miss1.txt ) ,
			7:9 
		)

		miss2.txt <-
			"SC11Qa1 SC11Qa2 SC11Qa3 SC11Qa4 SC11Qb1 SC11Qb2 SC11Qb3 SC11Qb4 SC11Qc1 SC11Qc2 SC11Qc3 SC11Qc4               
			SC11Qd1 SC11Qd2 SC11Qd3 SC11Qd4 SC11Qe1 SC11Qe2 SC11Qe3 SC11Qe4 SC11Qf1 SC11Qf2 SC11Qf3 SC11Qf4               
			SC11Qg1 SC11Qg2 SC11Qg3 SC11Qg4 SC11Qh1 SC11Qh2 SC11Qh3 SC11Qh4 SC11Qi1 SC11Qi2 SC11Qi3 SC11Qi4               
			SC11Qj1 SC11Qj2 SC11Qj3 SC11Qj4 SC11Qk1 SC11Qk2 SC11Qk3 SC11Qk4 SC11Ql1 SC11Ql2 SC11Ql3 SC11Ql4               
			SC12Qa1 SC12Qa2 SC12Qa3 SC12Qa4 SC12Qb1 SC12Qb2 SC12Qb3 SC12Qb4 SC12Qc1 SC12Qc2 SC12Qc3 SC12Qc4               
			SC12Qd1 SC12Qd2 SC12Qd3 SC12Qd4 SC12Qe1 SC12Qe2 SC12Qe3 SC12Qe4 SC12Qf1 SC12Qf2 SC12Qf3 SC12Qf4"

		pisa_missing.updates( 
			db , 
			'INT_Sch06_Dec07' , 
			pisa_split.n.clean( miss2.txt ) ,
			7:9 
		)

		pisa_missing.updates( 
			db , 
			'INT_Sch06_Dec07' , 
			c( "ABGROUP" , "SELSCH" , "SCHLTYPE" ) ,
			c( 7 , 9 ) 
		)


		pisa_missing.updates( 
			db , 
			'INT_Sch06_Dec07' , 
			c( "SC06Q01" , "CLSIZE" ) ,
			97:99
		)

		pisa_missing.updates( 
			db , 
			'INT_Sch06_Dec07' , 
			c( "RESPRES" , "RESPCURR" , "TCSHORT" , "SCMATEDU" , "SCIPROM" , "ENVLEARN" ) ,
			c( 997 , 999 )
		)

		miss6.txt <-
		"SC01Q01 SC01Q02 SC09Q11 SC09Q12 SC09Q21 SC09Q22 SC09Q31 SC09Q32 SC13Q01           
		SC13Q02 SC13Q03 SC03Q01 SC03Q02 SC03Q03 SC03Q04 PCGIRLS RATCOMP IRATCOMP COMPWEB  
		STRATIO PROPCERT PROPQUAL SC05Q01 SC05Q02"

		pisa_missing.updates( 
			db , 
			'INT_Sch06_Dec07' , 
			pisa_split.n.clean( miss6.txt ) ,
			c( 996 , 9997:9999 )
		)

		pisa_missing.updates( 
			db , 
			'INT_Sch06_Dec07' , 
			"SCHSIZE" ,
			99997:99999
		)

	}

pisa_int_par06_dec07.missings <-
	function( db ){

		miss1.txt <-
			"PA01Q01 PA01Q02 PA01Q03 PA02Q01 PA02Q02 PA02Q03 PA02Q04 PA02Q05             
			PA03Q01 PA03Q02 PA03Q03 PA03Q04 PA03Q05 PA03Q06 PA03Q07                     
			PA04Q01 PA04Q02 PA04Q03 PA04Q04 PA05Q01 PA05Q02 PA05Q03 PA05Q04 PA05Q05     
			PA06Q01 PA06Q02 PA06Q03 PA06Q04 PA06Q05 PA06Q06 PA06Q07 PA06Q08 PA06Q09     
			PA07Q01 PA07Q02 PA07Q03 PA07Q04 PA07Q05 PA07Q06                             
			PA08Q01 PA08Q02 PA08Q03 PA08Q04 PA08Q05 PA08Q06                             
			PA10Q01 PA10Q02 PA12Q01 PA12Q02 PA12Q03 PA12Q04                             
			PA14Q01 PA14Q02 PA14Q03 PA14Q04 PA15Q01 PQSRC_M PQSRC_F PQSRC_E PQFISCED PQMISCED PQHISCED"
			
		pisa_missing.updates( 
			db , 
			'int_par06_dec07' , 
			pisa_split.n.clean( miss1.txt ) ,
			7:9 
		)

		pisa_missing.updates( 
			db , 
			'int_par06_dec07' , 
			c( "PQBMMJ" , "PQBFMJ" , "PQHISEI" ) ,
			97:99
		)
			
		miss4 <- c( "PQENPERC" , "PQENVOPT" , "PQGENSCI" , "PQPERSCI" , "PQSCCAR" , "PQSCHOOL" , "PQSCIACT" , "PQSCIMP" )

		pisa_missing.updates( 
			db , 
			'int_par06_dec07' , 
			miss4 ,
			c( 9 , 997:999 )
		)

	}
	
	
	
pisa_int_cogn06_s_dec07.missings <-
	function( db ){

		miss1.txt <-
			"M033Q01 M034Q01T M155Q01 M155Q02T M155Q03T M155Q04T M192Q01T M273Q01T M302Q01T M302Q02 M302Q03 M305Q01 M406Q01 M406Q02 M408Q01T M411Q01 M411Q02 M420Q01T M421Q01 M421Q02T M421Q03 M423Q01 M442Q02 M446Q01 M446Q02 M447Q01 M462Q01T M464Q01T M474Q01 M496Q01T M496Q02 M559Q01 M564Q01 M564Q02 M571Q01 M598Q01 M603Q01T M603Q02T M710Q01 M800Q01 M803Q01T M810Q01T M810Q02T M810Q03T M828Q01 M828Q02 M828Q03 M833Q01T R055Q01 R055Q02 R055Q03 R055Q05 R067Q01 R067Q04 R067Q05 R102Q04A R102Q05 R102Q07 R104Q01 R104Q02 R104Q05 R111Q01 R111Q02B R111Q06B R219Q01E R219Q01T R219Q02 R220Q01 R220Q02B R220Q04 R220Q05 R220Q06 R227Q01 R227Q02T R227Q03 R227Q06 S114Q03T S114Q04T S114Q05T S131Q02T S131Q04T S213Q01T S213Q02 S256Q01 S268Q01 S268Q02T S268Q06 S269Q01 S269Q03T S269Q04T S304Q01 S304Q02 S304Q03A S304Q03B S326Q01 S326Q02 S326Q03 S326Q04T S408Q01 S408Q03 S408Q04T S408Q05 S413Q04T S413Q05 S413Q06 S415Q02 S415Q07T S415Q08T S416Q01 S421Q01 S421Q03 S425Q02 S425Q03 S425Q04 S425Q05 S426Q03 S426Q05 S426Q07T S428Q01 S428Q03 S428Q05 S437Q01 S437Q03 S437Q04 S437Q06 S438Q01T S438Q02 S438Q03T S447Q02 S447Q03 S447Q04 S447Q05 S458Q01 S458Q02T S465Q01 S465Q02 S465Q04 S466Q01T S466Q05 S466Q07T S476Q01 S476Q02 S476Q03 S477Q02 S477Q03 S477Q04 S478Q01 S478Q02T S478Q03T S485Q02 S485Q03 S485Q05 S493Q01T S493Q03T S493Q05T S495Q01T S495Q02T S495Q03 S495Q04T S498Q02T S498Q03 S498Q04 S508Q02T S508Q03 S510Q01T S510Q04T S514Q02 S514Q03 S514Q04 S519Q01 S519Q02T S519Q03 S521Q02 S521Q06 S524Q06T S524Q07 S527Q01T S527Q03T S527Q04T S408QNA S408QNB S408QNC S413QNA S413QNB S413QNC S416QNA S416QNB S428QNA S428QNB S428QNC S437QNA S437QNB S437QNC S438QNA S438QNB S438QNC S456QNA S456QNB S456QNC S466QNA S466QNB S466QNC S476QNA S476QNB S476QNC S478QNA S478QNB S478QNC S485QNA S485QNB S485QNC S498QNA S498QNB S498QNC S508QNA S508QNB S508QNC S514QNA S514QNB S514QNC S519QNA S519QNB S519QNC S521QNA S521QNB S524QNA S524QNB S524QNC S527QNA S527QNB S527QNC S408QSA S408QSB S408QSC S416QSA S416QSB S416QSC S421QSA S421QSC S425QSA S425QSB S425QSC S426QSA S426QSB S426QSC S438QSA S438QSB S438QSC S456QSA S456QSB S456QSC S465QSA S465QSB S476QSA S476QSB S476QSC S477QSA S477QSB S477QSC S485QSB S485QSC S498QSA S498QSB S519QSA S519QSB S519QSC S527QSB S527QSC"

			
		pisa_missing.updates( 
			db , 
			'INT_Cogn06_S_Dec07' , 
			pisa_split.n.clean( miss1.txt ) ,
			7
		)
			
		pisa_missing.updates( 
			db , 
			'INT_Cogn06_S_Dec07' , 
			c( "CLCUSE3A" , "CLCUSE3B" , "DEFFORT" ) ,
			997:999
		)

	}

	
pisa_int_scq09_dec11.missings <-
	function( db ){
			
		miss1.txt <-
			"SC01Q01 SC01Q02 SC01Q03 SC01Q04 SC01Q05 SC01Q06 SC01Q07 SC01Q08 SC01Q09 SC01Q10 SC01Q11 SC01Q12 SC01Q13 SC01Q14 SC02Q01 SC04Q01 SC05Q01 SC08Q01 SC11Q01 SC11Q02 SC11Q03 SC11Q04 SC11Q05 SC11Q06 SC11Q07 SC11Q08 SC11Q09 SC11Q10 SC11Q11 SC11Q12 SC11Q13 SC12Q01 SC12Q02 SC13Q01 SC13Q02 SC13Q03 SC13Q04 SC13Q05 SC13Q06 SC13Q07 SC13Q08 SC13Q09 SC13Q10 SC13Q11 SC13Q12 SC13Q13 SC13Q14 SC14Q01 SC14Q02 SC14Q03 SC14Q04 SC14Q05 SC15Q01 SC15Q02 SC15Q03 SC15Q04 SC15Q05 SC16Q01 SC16Q02 SC16Q03 SC16Q04 SC16Q05 SC16Q06 SC16Q07 SC16Q08 SC17Q01 SC17Q02 SC17Q03 SC17Q04 SC17Q05 SC17Q06 SC17Q07 SC17Q08 SC17Q09 SC17Q10 SC17Q11 SC17Q12 SC17Q13 SC18Q01 SC19Q01 SC19Q02 SC19Q03 SC19Q04 SC19Q05 SC19Q06 SC19Q07 SC20Q01 SC20Q02 SC20Q03 SC20Q04 SC20Q05 SC20Q06 SC21Q01 SC21Q02 SC21Q03 SC22Q01 SC22Q02 SC22Q03 SC22Q04 SC22Q05 SC23Q01 SC23Q02 SC23Q03 SC23Q04 SC26Q01 SC26Q02 SC26Q03 SC26Q04 SC26Q05 SC26Q06 SC26Q07 SC26Q08 SC26Q09 SC26Q10 SC26Q11 SC26Q12 SC26Q13 SC26Q14 SC27Q01 abgroup SCHTYPE selsch SC24Qa1  SC24Qa2  SC24Qa3  SC24Qa4  SC24Qa5  SC24Qb1  SC24Qb2  SC24Qb3  SC24Qb4  SC24Qb5 SC24Qc1  SC24Qc2  SC24Qc3  SC24Qc4  SC24Qc5  SC24Qd1  SC24Qd2  SC24Qd3  SC24Qd4  SC24Qd5 SC24Qe1  SC24Qe2  SC24Qe3  SC24Qe4  SC24Qe5  SC24Qf1  SC24Qf2  SC24Qf3  SC24Qf4  SC24Qf5 SC24Qg1  SC24Qg2  SC24Qg3  SC24Qg4  SC24Qg5  SC24Qh1  SC24Qh2  SC24Qh3  SC24Qh4  SC24Qh5 SC24Qi1  SC24Qi2  SC24Qi3  SC24Qi4  SC24Qi5  SC24Qj1  SC24Qj2  SC24Qj3  SC24Qj4  SC24Qj5 SC24Qk1  SC24Qk2  SC24Qk3  SC24Qk4  SC24Qk5  SC24Ql1  SC24Ql2  SC24Ql3  SC24Ql4  SC24Ql5 SC25Qa1  SC25Qa2  SC25Qa3  SC25Qa4  SC25Qb1  SC25Qb2  SC25Qb3  SC25Qb4  SC25Qc1  SC25Qc2 SC25Qc3  SC25Qc4  SC25Qd1  SC25Qd2  SC25Qd3  SC25Qd4  SC25Qe1  SC25Qe2  SC25Qe3  SC25Qe4 SC25Qf1  SC25Qf2  SC25Qf3  SC25Qf4"

		pisa_missing.updates( 
			db , 
			'INT_SCQ09_DEC11' , 
			pisa_split.n.clean( miss1.txt ) ,
			7:9 
		)

		miss6.txt <-
			"COMPWEB EXCURACT LDRSHP PCGIRLS PROPCERT PROPQUAL IRATCOMP RESPCURR RESPRES SC03Q01 SC03Q02 SC03Q03 SC03Q04 SC06Q01 SC06Q02 SC07Q01 SC07Q02 SC09Q11 SC09Q12 SC09Q21 SC09Q22 SC09Q31 SC09Q32 SC10Q01 SC10Q02 SC10Q03 SCMATEDU STRATIO STUDBEHA TCHPARTI TCSHORT TEACBEHA W_FSCHWT"

		pisa_missing.updates( 
			db , 
			'INT_SCQ09_DEC11' , 
			pisa_split.n.clean( miss6.txt ) ,
			c( 9997:9999 , 996 )
		)

		pisa_missing.updates( db , 'INT_SCQ09_DEC11' , "SCHSIZE" , 99997:99999 )

	}
	


pisa_int_stq09_dec11.missings <-
	function( db ){
			
		miss1.txt <-
			"ST04Q01 ST05Q01 ST07Q01 ST07Q02 ST07Q03 ST08Q01 ST08Q02 ST08Q03 ST08Q04 ST08Q05 ST08Q06 ST10Q01 ST11Q01 ST11Q02 ST11Q03 ST11Q04 ST12Q01 ST14Q01 ST15Q01 ST15Q02 ST15Q03 ST15Q04 ST16Q01 ST17Q01 ST17Q02 ST17Q03 ST19Q01 ST20Q01 ST20Q02 ST20Q03 ST20Q04 ST20Q05 ST20Q06 ST20Q07 ST20Q08 ST20Q09 ST20Q10 ST20Q11 ST20Q12 ST20Q13 ST20Q14 ST21Q01 ST21Q02 ST21Q03 ST21Q04 ST21Q05 ST22Q01 ST23Q01 ST24Q01 ST24Q02 ST24Q03 ST24Q04 ST24Q05 ST24Q06 ST24Q07 ST24Q08 ST24Q09 ST24Q10 ST24Q11 ST25Q01 ST25Q02 ST25Q03 ST25Q04 ST25Q05 ST26Q01 ST26Q02 ST26Q03 ST26Q04 ST26Q05 ST26Q06 ST26Q07 ST27Q01 ST27Q02 ST27Q03 ST27Q04 ST27Q05 ST27Q06 ST27Q07 ST27Q08 ST27Q09 ST27Q10 ST27Q11 ST27Q12 ST27Q13 ST31Q01 ST31Q02 ST31Q03 ST31Q04 ST31Q05 ST31Q06 ST31Q07 ST31Q08 ST31Q09 ST32Q01 ST32Q02 ST32Q03 ST32Q04 ST33Q01 ST33Q02 ST33Q03 ST33Q04 ST34Q01 ST34Q02 ST34Q03 ST34Q04 ST34Q05 ST36Q01 ST36Q02 ST36Q03 ST36Q04 ST36Q05 ST37Q01 ST37Q02 ST37Q03 ST37Q04 ST37Q05 ST37Q06 ST37Q07 ST38Q01 ST38Q02 ST38Q03 ST38Q04 ST38Q05 ST38Q06 ST38Q07 ST38Q08 ST38Q09 ST39Q01 ST39Q02 ST39Q03 ST39Q04 ST39Q05 ST39Q06 ST39Q07 ST40Q01 ST41Q01 ST41Q02 ST41Q03 ST41Q04 ST41Q05 ST41Q06 ST42Q01 ST42Q02 ST42Q03 ST42Q04 ST42Q05 IC01Q01 IC01Q02 IC01Q03 IC01Q04 IC01Q05 IC01Q06 IC01Q07 IC01Q08 IC02Q01 IC02Q02 IC02Q03 IC02Q04 IC02Q05 IC03Q01 IC04Q01 IC04Q02 IC04Q03 IC04Q04 IC04Q05 IC04Q06 IC04Q07 IC04Q08 IC04Q09 IC05Q01 IC05Q02 IC05Q03 IC05Q04 IC05Q05 IC06Q01 IC06Q02 IC06Q03 IC06Q04 IC06Q05 IC06Q06 IC06Q07 IC06Q08 IC06Q09 IC07Q01 IC07Q02 IC07Q03 IC07Q04 IC08Q01 IC08Q02 IC08Q03 IC08Q04 IC08Q05 IC09Q01 IC10Q01 IC10Q02 IC10Q03 IC10Q04 EC01Q01 EC02Q01 EC03Q01 EC04Q01 EC05Q01A EC05Q01B EC05Q01C EC05Q01D EC05Q01E EC05Q01F EC06Q01 EC06Q02 EC06Q03 RFS1Q01 RFS1Q02 RFS1Q03 RFS1Q04 RFS1Q05 RFS1Q06 RFS1Q07 RFS1Q08 RFS2Q01 RFS2Q02 RFS2Q03 RFS2Q04 RFS2Q05 RFS2Q06 RFS2Q07 RFS2Q08 RFS2Q09 FISCED MISCED HISCED ISCEDL ISCEDO ISCEDD FAMSTRUC IMMIG MsECATEG  FsECATEG HsECATEG"

		pisa_missing.updates( 
			db , 
			'INT_STQ09_DEC11' , 
			pisa_split.n.clean( miss1.txt ) ,
			7:9 
		)

		miss4.txt <-
			"ST06Q01 ST18Q01 ST28Q01 ST28Q02 ST28Q03 ST29Q01 ST29Q02 ST29Q03 ST30Q01 ST35Q01 EC07Q01 ICTRES ICTHOME ICTSCH ENTUSE HOMSCH USESCH HIGHCONF ATTCOMP JOYREAD  WEALTH HEDRES CULTPOSS HOMEPOS DIVREAD ONLNREAD MEMOR ELAB CSTRAT  ATSCHL STUDREL DISCLIMA STIMREAD STRSTRAT LIBUSE UNDREM METASUM ESCS LMINS MMINS SMINS RFSINTRP RFSNCONT RFSTRLIT RFSFUMAT LMINS  MMINS  SMINS"

		pisa_missing.updates( 
			db , 
			'INT_STQ09_DEC11' , 
			pisa_split.n.clean( miss4.txt ) ,
			9997:9999 
		)


		pisa_missing.updates( 
			db , 
			'INT_STQ09_DEC11' , 
			c( "ST02Q01" , "BookID" , "AGE" , "BMMJ" , "BFMJ" , "HISEI" , "PARED" , "GRADE" ) ,
			97:99 
		)

		pisa_missing.updates( 
			db , 
			'INT_STQ09_DEC11' , 
			"ST01Q01" ,
			96:99 
		)

	}



pisa_int_cogn06_t_dec07.missings <-
	function( db ){
		
	seven.to.n.txt <-
		"M033Q01 M034Q01T M155Q01 M155Q02T M155Q03T M155Q04T M192Q01T M273Q01T M302Q01T M302Q02 M302Q03 M305Q01 M406Q01 M406Q02 M408Q01T M411Q01 M411Q02 M420Q01T M421Q01 M421Q02T M421Q03 M423Q01 M442Q02 M446Q01 M446Q02 M447Q01 M462Q01T M464Q01T M474Q01 M496Q01T M496Q02 M559Q01 M564Q01 M564Q02 M571Q01 M598Q01 M603Q01T M603Q02T M710Q01 M800Q01 M803Q01T M810Q01T M810Q02T M810Q03T M828Q01 M828Q02 M828Q03 M833Q01T R055Q01 R055Q02 R055Q03 R055Q05 R067Q01 R067Q04 R067Q05 R102Q04A R102Q05 R102Q07 R104Q01 R104Q02 R104Q05 R111Q01 R111Q02B R111Q06B R219Q01E R219Q01T R219Q02 R220Q01 R220Q02B R220Q04 R220Q05 R220Q06 R227Q01 R227Q02T R227Q03 R227Q06 S114Q03T S114Q04T S114Q05T S131Q02T S131Q04T S213Q01T S213Q02 S256Q01 S268Q01 S268Q02T S268Q06 S269Q01 S269Q03T S269Q04T S304Q01 S304Q02 S304Q03A S304Q03B S326Q01 S326Q02 S326Q03 S326Q04T S408Q01 S408Q03 S408Q04T S408Q05 S413Q04T S413Q05 S413Q06 S415Q02 S415Q07T S415Q08T S416Q01 S421Q01 S421Q03 S425Q02 S425Q03 S425Q04 S425Q05 S426Q03 S426Q05 S426Q07T S428Q01 S428Q03 S428Q05 S437Q01 S437Q03 S437Q04 S437Q06 S438Q01T S438Q02 S438Q03T S447Q02 S447Q03 S447Q04 S447Q05 S458Q01 S458Q02T S465Q01 S465Q02 S465Q04 S466Q01T S466Q05 S466Q07T S476Q01 S476Q02 S476Q03 S477Q02 S477Q03 S477Q04 S478Q01 S478Q02T S478Q03T S485Q02 S485Q03 S485Q05 S493Q01T S493Q03T S493Q05T S495Q01T S495Q02T S495Q03 S495Q04T S498Q02T S498Q03 S498Q04 S508Q02T S508Q03 S510Q01T S510Q04T S514Q02 S514Q03 S514Q04 S519Q01 S519Q02T S519Q03 S521Q02 S521Q06 S524Q06T S524Q07 S527Q01T S527Q03T S527Q04T"

	for ( i in pisa_split.n.clean( seven.to.n.txt ) ){
		DBI::dbSendQuery(
			db ,
			paste(
				"UPDATE INT_Cogn06_T_Dec07 SET" ,
				i ,
				" = 'N' WHERE" ,
				i , 
				" = '7'"
			)
		)
	}

	miss1.txt <-
		"S408QNA S408QNB S408QNC S413QNA S413QNB S413QNC S416QNA S416QNB S428QNA S428QNB S428QNC S437QNA S437QNB S437QNC S438QNA S438QNB S438QNC S456QNA S456QNB S456QNC S466QNA S466QNB S466QNC S476QNA S476QNB S476QNC S478QNA S478QNB S478QNC S485QNA S485QNB S485QNC S498QNA S498QNB S498QNC S508QNA S508QNB S508QNC S514QNA S514QNB S514QNC S519QNA S519QNB S519QNC S521QNA S521QNB S524QNA S524QNB S524QNC S527QNA S527QNB S527QNC S408QSA S408QSB S408QSC S416QSA S416QSB S416QSC S421QSA S421QSC S425QSA S425QSB S425QSC S426QSA S426QSB S426QSC S438QSA S438QSB S438QSC S456QSA S456QSB S456QSC S465QSA S465QSB S476QSA S476QSB S476QSC S477QSA S477QSB S477QSC S485QSB S485QSC S498QSA S498QSB S519QSA S519QSB S519QSC S527QSB S527QSC"

	for ( i in pisa_split.n.clean( seven.to.n.txt ) ){
		DBI::dbSendQuery(
			db ,
			paste(
				"UPDATE INT_Cogn06_T_Dec07 SET" ,
				i ,
				" = 'N' WHERE" ,
				i , 
				" = '7'"
			)
		)

		DBI::dbSendQuery(
			db ,
			paste(
				"UPDATE INT_Cogn06_T_Dec07 SET" ,
				i ,
				" = 'I' WHERE" ,
				i , 
				" = '8'"
			)
		)

		DBI::dbSendQuery(
			db ,
			paste(
				"UPDATE INT_Cogn06_T_Dec07 SET" ,
				i ,
				" = 'M' WHERE" ,
				i , 
				" = '9'"
			)
		)
	
	}
			
	n.outs <- 
		read.csv( header = FALSE , 
			text = "M034R01,9997
				M155R02,97
				M155R03,97
				M155R04,7777
				M192R01,777
				M273R01,9997
				M302R01,9997
				M408R01,7777
				M420R01,7777
				M421R02,7777
				M462R01,97
				M464R01,9997
				M496R01,7777
				M603R01,777
				M603R02,9997
				M803R01,9997
				M810R01,9997
				M810R02,9997
				M810R03,97
				M833R01,77777
				R219R01,7777
				R227R02,7777777
				S114R03,97
				S114R05,97
				S131R02,97
				S131R04,97
				S213R01,7777
				S269R03,97
				S269R04,7777
				S326R04,777
				S408R04,777
				S413R04,777
				S415R07,77
				S415R08,777
				S426R07,77
				S438R01,777
				S438R03,97
				S458R02,777
				S466R01,777
				S466R07,77
				S478R02,777
				S478R03,77
				S493R01,777
				S493R03,77
				S493R05,97
				S495R01,777
				S495R02,77
				S495R04,777
				S498R02,777
				S508R02,77
				S510R01,77
				S510R04,97
				S519R02,77
				S524R06,77
				S527R01,777
				S527R03,77
				S527R04,777" ,
				colClasses = c( 'character' , 'numeric' )
		)

		# grab all column types
		tid <- DBI::dbReadTable( db , 'tables' )
		tid <- tid[ tid$name == 'int_cogn06_t_dec07' , 'id' ]

		cols <- DBI::dbReadTable( db , 'columns' )
		ctypes <- cols[ cols$table_id == tid , c( 'name' , 'type' ) ]

		n.outs[ , 1 ] <- tolower( n.outs[ , 1 ] )

		n.outs <- merge( n.outs , ctypes , by.x = 'V1' , by.y = 'name' )
		
		for ( i in seq( nrow( n.outs ) ) ){

			if( DBI::dbDataType( db , n.outs[ i , 3 ] ) %in% c( 'clob' , 'string' , 'varchar' ) ) {

				DBI::dbSendQuery(
					db ,
					paste0(
						"UPDATE INT_Cogn06_T_Dec07 SET " ,
						n.outs[ i , 1 ] ,
						" = 'N' WHERE " ,
						n.outs[ i , 1 ] , 
						" = '" ,
						n.outs[ i , 2 ] ,
						"'"
					)
				)
			
			} else {
			
				DBI::dbSendQuery(
					db ,
					paste0(
						"UPDATE INT_Cogn06_T_Dec07 SET " ,
						n.outs[ i , 1 ] ,
						" = NULL WHERE " ,
						n.outs[ i , 1 ] , 
						" = " ,
						n.outs[ i , 2 ]
					)
				)
			
			}
		}
		
		
		pisa_missing.updates( 
			db , 
			'int_cogn06_t_dec07' , 
			c( "CLCUSE3A" , "CLCUSE3B" , "DEFFORT" ) ,
			997:999
		)

	}

pisa_int_schi_2003.missings <-
	function( db ){
	
			
		miss3.txt <-
			"SC02Q01 SC02Q02 SC04Q01 SC04Q02 SC04Q03 SC04Q04 SC06Q01 SC06Q02 SC18Q11 SC18Q21 SC18Q12 SC18Q22 SC18Q13 SC18Q23 SC19Q11 SC19Q21 SC19Q12 SC19Q22 SC19Q13 SC19Q23 SC19Q14 SC19Q24 SC19Q15 SC19Q25 pcgirls tcshort scmatbui scmatedu stmorale tcmorale studbeha teacbeha tchcons schauton tchparti Scweight STRATIO PROPCERT PROPQPED SMRATIO PROPMATH PROPMA5A"

		pisa_missing.updates( 
			db , 
			'int_schI_2003' , 
			pisa_split.n.clean( miss3.txt ) ,
			997:999
		)

		pisa_missing.updates( 
			db , 
			'int_schI_2003' , 
			c( "SC09Q01" , "SC09Q02" , "SC09Q03" , "SC09Q04" , "SC09Q05" , "SC09Q06" ) ,
			9997:9999
		)

		pisa_missing.updates( db , 'int_schI_2003' , "RATCOMP" , 97:99 )


		pisa_missing.updates( db , 'int_schI_2003' , "SCHLSIZE" , 99997:99999 )

		miss1.txt <-
			"SC03Q01 SC05Q01 SC05Q02 SC05Q03 SC05Q04 SC05Q05 SC05Q06 SC05Q07 SC05Q08  SC05Q09 SC05Q10 SC05Q11 SC05Q12 SC05Q13 SC05Q14 SC08Q01 SC08Q02 SC08Q03 SC08Q04 SC08Q05 SC08Q06 SC08Q07 SC08Q08 SC08Q09 SC08Q10 SC08Q11 SC08Q12 SC08Q13 SC08Q14 SC08Q15 SC08Q16 SC08Q17 SC08Q18 SC08Q19 SC08Q20 SC10Q01 SC10Q02 SC10Q03 SC10Q04 SC10Q05 SC10Q06  SC10Q07 SC11Q01 SC11Q02 SC11Q03 SC11Q04 SC11Q05 SC11Q06 SC11Q07 SC12Q01 SC12Q02 SC12Q03 SC12Q04 SC12Q05 SC13Q01 SC13Q02 SC13Q03  SC13Q04 SC13Q05 SC13Q06 SC13Q07 SC13Q08 SC14Q01 SC15Q01 SC15Q02  SC16Q01 SC16Q02 SC16Q03 SC16Q04 SC17Q01 SC17Q02 SC17Q03 SC17Q04  SC17Q05 SC20Q01 SC20Q02 SC20Q03 SC20Q04 SC21Q01 SC21Q02 SC21Q03  SC22Q01 SC22Q02 SC22Q03 SC23Q01 SC23Q02 SC23Q03 SC24Q01 SC24Q02  SC24Q03 SC24Q04 SC25Q01 SC25Q02 SC25Q03 SC25Q04 SC25Q05 SC25Q06  SC25Q07 SC25Q08 SC25Q09 SC25Q10 SC25Q11 SC25Q12 SC25Q13 EXCOURSE MACTIV AUTRES AUTCURR MSTREL SC01Q01 SCHLTYPE compweb complan assess select_ abgroup"

		pisa_missing.updates( 
			db , 
			'int_schI_2003' , 
			pisa_split.n.clean( miss1.txt ) ,
			7:9
		)

		for ( i in pisa_split.n.clean( "SC26Q01 SC26Q02 SC26Q03 SC26Q04 SC26Q05 SC26Q06 SC26Q07 SC26Q08 SC26Q09 SC26Q10 SC26Q11 SC26Q12" ) ){

			DBI::dbSendQuery(
				db ,
				paste(
					"UPDATE int_schi_2003 SET" ,
					i ,
					"= 'N' WHERE" ,
					i ,
					"IN ( '77777' , '' , ' ' )"
				)
			)
			
			DBI::dbSendQuery(
				db ,
				paste(
					"UPDATE int_schi_2003 SET" ,
					i ,
					"= 'I' WHERE" ,
					i ,
					"= '88888'"
				)
			)
			
			DBI::dbSendQuery(
				db ,
				paste(
					"UPDATE int_schi_2003 SET" ,
					i ,
					"= 'M' WHERE" ,
					i ,
					"= '99999'"
				)
			)

		}

		for ( i in pisa_split.n.clean( "SC27Q01 SC27Q02 SC27Q03 SC27Q04 SC27Q05 SC27Q06 SC27Q07" ) ){

			DBI::dbSendQuery(
				db ,
				paste(
					"UPDATE int_schi_2003 SET" ,
					i ,
					"= 'N' WHERE" ,
					i ,
					"IN ( '7777' , '' , ' ' )"
				)
			)
			
			DBI::dbSendQuery(
				db ,
				paste(
					"UPDATE int_schi_2003 SET" ,
					i ,
					"= 'I' WHERE" ,
					i ,
					"= '8888'"
				)
			)
			
			DBI::dbSendQuery(
				db ,
				paste(
					"UPDATE int_schi_2003 SET" ,
					i ,
					"= 'M' WHERE" ,
					i ,
					"= '9999'"
				)
			)

		}

	}
	
	
pisa_int_stui_2003_v2.missings <-
	function( db ){

		miss1.txt <-
			"st03q01 st04q01 st04q02 st04q03 st04q04 st04q05 st05q01 st06q01 st12q01 st12q02 st12q03 st14q01 st14q02 st14q03 st17q01 st17q02 st17q03 st17q04 st17q05 st17q06 st17q07 st17q08 st17q09 st17q10 st17q11 st17q12 st17q13 st19q01 st20q01 st22q01 st22q02 st22q03 st23q01 st23q02 st23q03 st23q04 st23q05 st23q06 st24q01 st24q02 st24q03 st24q04 st25q01 st25q02 st25q03 st25q04 st25q05 st25q06 st26q01 st26q02 st26q03 st26q04 st26q05 st27q01 st27q02 st27q03 st27q04 st27q05 st27q06 st28q01 st30q01 st30q02 st30q03 st30q04 st30q05 st30q06 st30q07 st30q08 st31q01 st31q02 st31q03 st31q04 st31q05 st31q06 st31q07 st31q08 st32q01 st32q02 st32q03 st32q04 st32q05 st32q06 st32q07 st32q08 st32q09 st32q10 st34q01 st34q02 st34q03 st34q04 st34q05 st34q06 st34q07 st34q08 st34q09 st34q10 st34q11 st34q12 st34q13 st34q14 st37q01 st37q02 st37q03 st37q04 st37q05 st37q06 st37q07 st37q08 st37q09 st37q10 st38q01 st38q02 st38q03 st38q04 st38q05 st38q06 st38q07 st38q08 st38q09 st38q10 st38q11 ec01q01 ec02q01 ec03q01 ec04q01 ec05q01 ec06q01 ec07q02 ic01q01 ic01q02 ic01q03 ic02q01 ic03q01 ic04q01 ic04q02 ic04q03 ic05q01 ic05q02 ic05q03 ic05q04 ic05q05 ic05q06 ic05q07 ic05q08 ic05q09 ic05q10 ic05q11 ic05q12 ic06q01 ic06q02 ic06q03 ic06q04 ic06q05 ic06q06 ic06q07 ic06q08 ic06q09 ic06q10 ic06q11 ic06q12 ic06q13 ic06q14 ic06q15 ic06q16 ic06q17 ic06q18 ic06q19 ic06q20 ic06q21 ic06q22 ic06q23 ic07q01 ic07q02 ic07q03 ic07q04 ic08q01 ic09q01  iscedl iscedd iscedo GRADE FAMSTRUC misced  fisced HISCED immig sisced  MSECateg FSECateg HSECateg SSECateg ST11R01  ST13R01 lang"
			
		pisa_missing.updates( 
			db , 
			'int_stuI_2003_v2' , 
			pisa_split.n.clean( miss1.txt ) ,
			7:9 
		)

		miss3.txt <-
			"ST15Q04 EC07Q01 EC07Q03 MMINS TMINS PCMATH ST21Q01 ST29Q01 ST29Q02 ST29Q03 ST29Q04 ST29Q05 ST29Q06  ST33Q01 ST33Q02 ST33Q03 ST33Q04 ST33Q05 ST33Q06 
			ST35Q02 ST35Q03 ST36Q01 sc07q01 ANXMAT ATSCHL ATTCOMP BELONG COMPHOME COMPLRN COOPLRN CSTRAT CULTPOSS DISCLIM ELAB HEDRES HIGHCONF INSTMOT 
			INTCONF INTMAT INTUSE MATHEFF MEMOR PRGUSE ROUTCONF SCMAT STUREL TEACHSUP  rmhmwk  homepos ESCS CLCUSE3a CLCUSE3b"

		pisa_missing.updates( 
			db , 
			'int_stuI_2003_v2' , 
			pisa_split.n.clean( miss3.txt ) ,
			997:999 
		)

		miss2.txt <- "pared BMMJ BFMJ BSMJ AGE ST01Q01 ST02Q02 ST02Q03 ST15Q01 ST15Q02 ST15Q03 ST16Q01 HISEI"

		pisa_missing.updates( 
			db , 
			'int_stuI_2003_v2' , 
			pisa_split.n.clean( miss2.txt ) ,
			97:99 
		)

		for ( i in pisa_split.n.clean( "iso_s iso_m iso_f" ) ){

			DBI::dbSendQuery( db , paste( "UPDATE int_stuI_2003_v2 SET" , i , "= 'N' WHERE" , i , "IN ( '99999970' , '' , ' ' )" ) )
			DBI::dbSendQuery( db , paste( "UPDATE int_stuI_2003_v2 SET" , i , "= 'I' WHERE" , i , "= '99999980'" ) )
			DBI::dbSendQuery( db , paste( "UPDATE int_stuI_2003_v2 SET" , i , "= 'M' WHERE" , i , "= '99999990'" ) )
			
		}

		for ( i in pisa_split.n.clean( "ST07Q01 ST09Q01 EC08Q01 EC06Q02" ) ){

			DBI::dbSendQuery( db , paste( "UPDATE int_stuI_2003_v2 SET" , i , "= 'N' WHERE" , i , "IN ( '9997' , '' , ' ' )" ) )
			DBI::dbSendQuery( db , paste( "UPDATE int_stuI_2003_v2 SET" , i , "= 'I' WHERE" , i , "= '9998'" ) )
			DBI::dbSendQuery( db , paste( "UPDATE int_stuI_2003_v2 SET" , i , "= 'M' WHERE" , i , "= '9999'" ) )
			
		}

		for ( i in pisa_split.n.clean( "ST17Q14 ST17Q15 ST17Q16 LANGN" ) ){

			DBI::dbSendQuery( db , paste( "UPDATE int_stuI_2003_v2 SET" , i , "= 'N' WHERE" , i , "IN ( '99997' , '' , ' ' )" ) )
			DBI::dbSendQuery( db , paste( "UPDATE int_stuI_2003_v2 SET" , i , "= 'I' WHERE" , i , "= '99998'" ) )
			DBI::dbSendQuery( db , paste( "UPDATE int_stuI_2003_v2 SET" , i , "= 'M' WHERE" , i , "= '99999'" ) )
			
		}

	}
