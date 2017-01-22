get_catalog_cps_asec <-
	function( data_name = "cps_asec" , output_dir , ... ){

		cps_ftp <- "http://thedataweb.rm.census.gov/ftp/cps_ftp.html#cpsmarch"

		cps_links <- rvest::html_attr( rvest::html_nodes( xml2::read_html( cps_ftp ) , "a" ) , "href" )
		
		these_links <- grep( "asec(.*)zip$" , cps_links , value = TRUE , ignore.case = TRUE )

		asec_max_year <- max( as.numeric( substr( gsub( "[^0-9]" , "" , these_links ) , 1 , 4 ) ) )
		
		asec_years <- c( 1998:2014 , 2014.58 , 2014.38 , seq( 2015 , asec_max_year ) )
		
		catalog <-
			data.frame(
				year = asec_years ,
				db_tablename = paste0( "asec" , substr( asec_years , 3 , nchar( asec_years ) ) ) ,
				dbfolder = paste0( output_dir , "/MonetDB" ) ,
				stringsAsFactors = FALSE
			)

		# overwrite 2014.38 with three-eights
		catalog$db_tablename <- gsub( "\\.38" , "_3x8" , catalog$db_tablename )

		# overwrite 2014.58 with three-eights
		catalog$db_tablename <- gsub( "\\.58" , "_5x8" , catalog$db_tablename )

		catalog

	}


lodown_cps_asec <-
	function( data_name = "cps_asec" , catalog , ... ){

		tf <- tempfile()


		for ( i in seq_len( nrow( catalog ) ) ){
					
			# open the connection to the monetdblite database
			db <- DBI::dbConnect( MonetDBLite::MonetDBLite() , catalog[ i , 'dbfolder' ] )

			# # # # # # # # # # # #
			# load the main file  #
			# # # # # # # # # # # #

			# this process is slow.
			# for example, the CPS ASEC 2011 file has 204,983 person-records.

			# for the 2014 cps, load the income-consistent file as the full-catalog[ i , 'year' ] extract
			if ( catalog[ i , 'year' ] == 2014 ){
				
				tf1 <- tempfile() ; tf2 <- tempfile() ; tf3 <- tempfile()
			
				cachaca( "http://www.census.gov/housing/extract_files/data%20extracts/cpsasec14/hhld.sas7bdat" , tf1 , mode = 'wb' )
				cachaca( "http://www.census.gov/housing/extract_files/data%20extracts/cpsasec14/family.sas7bdat" , tf2 , mode = 'wb' )
				cachaca( "http://www.census.gov/housing/extract_files/data%20extracts/cpsasec14/person.sas7bdat" , tf3 , mode = 'wb' )

				hhld <- data.frame( haven::read_sas( tf1 ) )
				names( hhld ) <- tolower( names( hhld ) )
				for ( j in names( hhld ) ) hhld[ , j ] <- as.numeric( hhld[ , j ] )
				hhld$hsup_wgt <- hhld$hsup_wgt / 100
				DBI::dbWriteTable( db , 'hhld' , hhld )
				rm( hhld ) ; gc() ; file.remove( tf1 )
				
				fmly <- data.frame( haven::read_sas( tf2 ) )
				names( fmly ) <- tolower( names( fmly ) )
				for ( j in names( fmly ) ) fmly[ , j ] <- as.numeric( fmly[ , j ] )
				fmly$fsup_wgt <- fmly$fsup_wgt / 100
				DBI::dbWriteTable( db , 'family' , fmly )
				rm( fmly ) ; gc() ; file.remove( tf2 )
				
				prsn <- data.frame( haven::read_sas( tf3 ) )
				names( prsn ) <- tolower( names( prsn ) )
				for ( j in names( prsn ) ) prsn[ , j ] <- as.numeric( prsn[ , j ] )
				for ( j in c( 'marsupwt' , 'a_ernlwt' , 'a_fnlwgt' ) ) prsn[ , j ] <- prsn[ , j ] / 100
				DBI::dbWriteTable( db , 'person' , prsn )
				rm( prsn ) ; gc() ; file.remove( tf3 )

				mmf <- DBI::dbListFields( db , 'person' )[ !( DBI::dbListFields( db , 'person' ) %in% DBI::dbListFields( db , 'family' ) ) ]
				DBI::dbSendQuery( db , paste( "create table f_p as select a.* ," , paste( "b." , mmf , sep = "" , collapse = "," ) , "from family as a inner join person as b on a.fh_seq = b.ph_seq AND a.ffpos = b.phf_seq" ) )
			
				mmf <- DBI::dbListFields( db , 'f_p' )[ !( DBI::dbListFields( db , 'f_p' ) %in% DBI::dbListFields( db , 'hhld' ) ) ]
				DBI::dbSendQuery( db , paste( "create table hfpz as select a.* ," , paste( "b." , mmf , sep = "" , collapse = "," ) , "from hhld as a inner join f_p as b on a.h_seq = b.ph_seq" ) )

				stopifnot( DBI::dbGetQuery( db , 'select count(*) from hfpz' )[ 1 , 1 ] == DBI::dbGetQuery( db , 'select count(*) from person' )[ 1 , 1 ] )

				DBI::dbRemoveTable( db , 'f_p' )
				
			} else {
				
				# note: this CPS March Supplement ASCII (fixed-width file) contains household-, family-, and person-level records.

				# census.gov website containing the current population survey's main file
				CPS.ASEC.mar.file.location <- 
					ifelse( 
						# if the catalog[ i , 'year' ] to download is 2007, the filename doesn't match the others..
						catalog[ i , 'year' ] == 2007 ,
						"http://thedataweb.rm.census.gov/pub/cps/march/asec2007_pubuse_tax2.zip" ,
						ifelse(
							catalog[ i , 'year' ] %in% 2004:2003 ,
							paste0( "http://thedataweb.rm.census.gov/pub/cps/march/asec" , catalog[ i , 'year' ] , ".zip" ) ,
							ifelse(
								catalog[ i , 'year' ] %in% 2002:1998 ,
								paste0( "http://thedataweb.rm.census.gov/pub/cps/march/mar" , substr( catalog[ i , 'year' ] , 3 , 4 ) , "supp.zip" ) ,
								ifelse( 
									catalog[ i , 'year' ] == 2014.38 ,
									"http://thedataweb.rm.census.gov/pub/cps/march/asec2014_pubuse_3x8_rerun_v2.zip" ,
									ifelse( 
										catalog[ i , 'year' ] == 2014.58 ,
										"http://thedataweb.rm.census.gov/pub/cps/march/asec2014_pubuse.zip" ,
										ifelse( catalog[ i , 'year' ] == 2016 ,
											paste0( "http://thedataweb.rm.census.gov/pub/cps/march/asec" , catalog[ i , 'year' ] , "early_pubuse_v2.zip" ) ,
											paste0( "http://thedataweb.rm.census.gov/pub/cps/march/asec" , catalog[ i , 'year' ] , "_pubuse.zip" )
										)
									)
								)
							)
						)
					)

				if( catalog[ i , 'year' ] < 2011 ){
					
					# national bureau of economic research website containing the current population survey's SAS import instructions
					CPS.ASEC.mar.SAS.read.in.instructions <- 
							ifelse(
								catalog[ i , 'year' ] %in% 1987 ,
								paste0( "http://www.nber.org/data/progs/cps/cpsmar" , catalog[ i , 'year' ] , ".sas" ) , 
								paste0( "http://www.nber.org/data/progs/cps/cpsmar" , substr( catalog[ i , 'year' ] , 3 , 4 ) , ".sas" ) 
							)

					# figure out the household, family, and person begin lines
					hh_beginline <- grep( "HOUSEHOLD RECORDS" , readLines( CPS.ASEC.mar.SAS.read.in.instructions ) )
					fa_beginline <- grep( "FAMILY RECORDS" , readLines( CPS.ASEC.mar.SAS.read.in.instructions ) )
					pe_beginline <- grep( "PERSON RECORDS" , readLines( CPS.ASEC.mar.SAS.read.in.instructions ) )

				} else {
					
					if( catalog[ i , 'year' ] >= 2016 ) sas_ris <- cps_asec_dd_parser( paste0( "http://thedataweb.rm.census.gov/pub/cps/march/asec" , catalog[ i , 'year' ] , "_pubuse.dd.txt" ) )
					if( catalog[ i , 'year' ] == 2015 ) sas_ris <- cps_asec_dd_parser( "http://thedataweb.rm.census.gov/pub/cps/march/asec2015early_pubuse.dd.txt" )
					if( catalog[ i , 'year' ] == 2014.38 ) sas_ris <- cps_asec_dd_parser( "http://thedataweb.rm.census.gov/pub/cps/march/asec2014R_pubuse.dd.txt" )
					if( catalog[ i , 'year' ] == 2014.58 ) sas_ris <- cps_asec_dd_parser( "http://thedataweb.rm.census.gov/pub/cps/march/asec2014early_pubuse.dd.txt" )
					if( catalog[ i , 'year' ] == 2013 ) sas_ris <- cps_asec_dd_parser( "http://thedataweb.rm.census.gov/pub/cps/march/asec2013early_pubuse.dd.txt" )
					if( catalog[ i , 'year' ] == 2012 ) sas_ris <- cps_asec_dd_parser( "http://thedataweb.rm.census.gov/pub/cps/march/asec2012early_pubuse.dd.txt" )
					if( catalog[ i , 'year' ] == 2011 ) sas_ris <- cps_asec_dd_parser( "http://thedataweb.rm.census.gov/pub/cps/march/asec2011_pubuse.dd.txt" )

				}
					
				# create a temporary file and a temporary directory..
				tf <- tempfile() ; td <- tempdir()

				# download the CPS repwgts zipped file to the local computer
				cachaca( CPS.ASEC.mar.file.location , tf , mode = "wb" )

				# unzip the file's contents and store the file name within the temporary directory
				fn <- unzip( tf , exdir = td , overwrite = TRUE )

				# create three more temporary files
				# to store household-, family-, and person-level records ..and also the crosswalk
				tf.household <- tempfile()
				tf.family <- tempfile()
				tf.person <- tempfile()
				tf.xwalk <- tempfile()
				
				# create four file connections.

				# one read-only file connection "r" - pointing to the ASCII file
				incon <- file( fn , "r") 

				# three write-only file connections "w" - pointing to the household, family, and person files
				outcon.household <- file( tf.household , "w") 
				outcon.family <- file( tf.family , "w") 
				outcon.person <- file( tf.person , "w") 
				outcon.xwalk <- file( tf.xwalk , "w" )
				
				# start line counter #
				line.num <- 0

				# store the current scientific notation option..
				cur.sp <- getOption( "scipen" )

				# ..and change it
				options( scipen = 10 )
				
				
				# figure out the ending position for each filetype
				# take the sum of the absolute value of the width parameter of the parsed-SAScii SAS input file, for household-, family-, and person-files separately
				if( catalog[ i , 'year' ] < 2011 ){
					end.household <- sum( abs( SAScii::parse.SAScii( CPS.ASEC.mar.SAS.read.in.instructions , beginline = hh_beginline )$width ) )
					end.family <- sum( abs( SAScii::parse.SAScii( CPS.ASEC.mar.SAS.read.in.instructions , beginline = fa_beginline )$width ) )
					end.person <- sum( abs( SAScii::parse.SAScii( CPS.ASEC.mar.SAS.read.in.instructions , beginline = pe_beginline )$width ) )
				} else {
					end.household <- sum( abs( sas_ris[[1]]$width ) )
					end.family <- sum( abs( sas_ris[[2]]$width ) )
					end.person <- sum( abs( sas_ris[[3]]$width ) )
				}
				
				# create a while-loop that continues until every line has been examined
				# cycle through every line in the downloaded CPS ASEC 20## file..

				while( length( line <- readLines( incon , 1 ) ) > 0 ){

					# ..and if the first character is a 1, add it to the new household-only CPS file.
					if ( substr( line , 1 , 1 ) == "1" ){
						
						# write the line to the household file
						writeLines( substr( line , 1 , end.household ) , outcon.household )
						
						# store the current unique household id
						curHH <- substr( line , 2 , 6 )
					
					}
					
					# ..and if the first character is a 2, add it to the new family-only CPS file.
					if ( substr( line , 1 , 1 ) == "2" ){
					
						# write the line to the family file
						writeLines( substr( line , 1 , end.family )  , outcon.family )
						
						# store the current unique family id
						curFM <- substr( line , 7 , 8 )
					
					}
					
					# ..and if the first character is a 3, add it to the new person-only CPS file.
					if ( substr( line , 1 , 1 ) == "3" ){
						
						# write the line to the person file
						writeLines( substr( line , 1 , end.person )  , outcon.person )
						
						# store the current unique person id
						curPN <- substr( line , 7 , 8 )
						
						writeLines( paste0( curHH , curFM , curPN ) , outcon.xwalk )
						
					}

					# add to the line counter #
					line.num <- line.num + 1

					# every 10k records..
					if ( line.num %% 10000 == 0 ) {
						
						# print current progress to the screen #
						cat( "   " , prettyNum( line.num  , big.mark = "," ) , "of approximately 400,000 cps asec lines processed" , "\r" )
						
					}
				}

				# restore the original scientific notation option
				options( scipen = cur.sp )

				# close all four file connections
				close( outcon.household )
				close( outcon.family )
				close( outcon.person )
				close( outcon.xwalk )
				close( incon , add = T )


				# the 2011 SAS file produced by the National Bureau of Economic Research (NBER)
				# begins each INPUT block after lines 988, 1121, and 1209, 
				# so skip SAS import instruction lines before that.
				# NOTE that this 'beginline' parameters of 988, 1121, and 1209 will change for different years.

				if( catalog[ i , 'year' ] < 2011 ){
					# store CPS ASEC march household records as a MonetDB database
					read_SAScii_monetdb ( 
						tf.household , 
						CPS.ASEC.mar.SAS.read.in.instructions , 
						beginline = hh_beginline , 
						zipped = FALSE ,
						tl = TRUE ,
						tablename = 'household' ,
						connection = db
					)

					# store CPS ASEC march family records as a MonetDB database
					read_SAScii_monetdb ( 
						tf.family , 
						CPS.ASEC.mar.SAS.read.in.instructions , 
						beginline = fa_beginline , 
						zipped = FALSE ,
						tl = TRUE ,
						tablename = 'family' ,
						connection = db
					)

					# store CPS ASEC march person records as a MonetDB database
					read_SAScii_monetdb ( 
						tf.person , 
						CPS.ASEC.mar.SAS.read.in.instructions , 
						beginline = pe_beginline , 
						zipped = FALSE ,
						tl = TRUE ,
						tablename = 'person' ,
						connection = db
					)
				} else {
					# store CPS ASEC march household records as a MonetDB database
					read_SAScii_monetdb ( 
						tf.household , 
						sas_stru = sas_ris[[1]] ,
						zipped = FALSE ,
						tl = TRUE ,
						tablename = 'household' ,
						connection = db
					)

					# store CPS ASEC march family records as a MonetDB database
					read_SAScii_monetdb ( 
						tf.family , 
						sas_stru = sas_ris[[2]] ,
						zipped = FALSE ,
						tl = TRUE ,
						tablename = 'family' ,
						connection = db
					)

					# store CPS ASEC march person records as a MonetDB database
					read_SAScii_monetdb ( 
						tf.person , 
						sas_stru = sas_ris[[3]] ,
						zipped = FALSE ,
						tl = TRUE ,
						tablename = 'person' ,
						connection = db
					)
				}

				

				# create a fake sas input script for the crosswalk..
				xwalk.sas <-
					"INPUT
						@1 h_seq 5.
						@6 ffpos 2.
						@8 pppos 2.
					;"
					
				# save it to the local disk
				xwalk.sas.tf <- tempfile()
				writeLines ( xwalk.sas , con = xwalk.sas.tf )

				
				# store CPS ASEC march xwalk records as a MonetDB database
				read_SAScii_monetdb ( 
					tf.xwalk , 
					xwalk.sas.tf , 
					zipped = FALSE ,
					tl = TRUE ,
					tablename = 'xwalk' ,
					connection = db
				)
				
				# clear up RAM
				gc()

				
				# create the merged file
				DBI::dbSendQuery( db , "create table h_xwalk as select a.ffpos , a.pppos , b.* from xwalk as a inner join household as b on a.h_seq = b.h_seq" )
				
				mmf <- DBI::dbListFields( db , 'family' )[ !( DBI::dbListFields( db , 'family' ) %in% DBI::dbListFields( db , 'h_xwalk' ) ) ]
				DBI::dbSendQuery( db , paste( "create table h_f_xwalk as select a.* , " , paste( "b." , mmf , sep = "" , collapse = "," ) , " from h_xwalk as a inner join family as b on a.h_seq = b.fh_seq AND a.ffpos = b.ffpos" ) )
			
				mmf <- DBI::dbListFields( db , 'person' )[ !( DBI::dbListFields( db , 'person' ) %in% DBI::dbListFields( db , 'h_f_xwalk' ) ) ]
				DBI::dbSendQuery( db , paste( "create table hfpz as select a.* , " , paste( "b." , mmf , sep = "" , collapse = "," ) , " from h_f_xwalk as a inner join person as b on a.h_seq = b.ph_seq AND a.pppos = b.pppos" ) )
			
			}
				
			# tack on _anycov_ variables
			# tack on _outtyp_ variables
			if( catalog[ i , 'year' ] > 2013 ){
				
				DBI::dbSendQuery( db , "create table hfp_pac as select * from hfpz" )
				
				DBI::dbRemoveTable( db , 'hfpz' )
				
				stopifnot( catalog[ i , 'year' ] %in% c( 2016 , 2015 , 2014.58 , 2014.38 , 2014 ) )
				
				tf <- tempfile()
				
				ac <- NULL
				
				ot <- NULL
				
				if( catalog[ i , 'year' ] %in% c( 2014 , 2014.58 ) ){
					
					ace <- "http://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2014/cps-redesign/asec14_now_anycov.dat"

					cachaca( ace , tf , mode = "wb" )	

					ac <- rbind( ac , read.fwf( tf , c( 5 , 2 , 1 ) ) )
					
				}

				if( catalog[ i , 'year' ] %in% c( 2014 , 2014.38 ) ){
					
					ace <- "http://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2014/cps-redesign/asec14_now_anycov_redes.dat"

					cachaca( ace , tf , mode = "wb" )	

					ac <- rbind( ac , read.fwf( tf , c( 5 , 2 , 1 ) ) )
					
				}
				
				if ( catalog[ i , 'year' ] %in% c( 2014 , 2014.38 , 2014.58 ) ){
				
					ote <- "http://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2014/cps-redesign/asec14_outtyp_full.dat"
					
					cachaca( ote , tf , mode = 'wb' )
					
					ot <- read.fwf( tf , c( 5 , 2 , 2 , 1 ) )
					
					cachaca( "http://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2014/cps-redesign/ppint14esi_offer_ext.sas7bdat" , tf , mode = 'wb' )
					
					offer <- data.frame( haven::read_sas( tf ) )
					
				}
				
				
				if ( catalog[ i , 'year' ] %in% 2015 ){
				
					ote <- "http://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2014/cps-redesign/asec15_outtyp.dat"
				
					cachaca( ote , tf , mode = 'wb' )
					
					ot <- read.fwf( tf , c( 5 , 2 , 2 , 1 ) )
					
					ace <- "http://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2014/cps-redesign/asec15_currcov_extract.dat"
				
					cachaca( ace , tf , mode = 'wb' )
					
					ac <- read.fwf( tf , c( 5 , 2 , 1 ) ) 

					cachaca( "http://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2014/cps-redesign/ppint15esi_offer_ext.sas7bdat" , tf , mode = 'wb' )
					
					offer <- data.frame( haven::read_sas( tf ) )
				
				}
				
				if ( catalog[ i , 'year' ] %in% 2016 ){
				
					ote <- "http://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2016/cps-redesign/asec16_outtyp_full.dat"
				
					cachaca( ote , tf , mode = 'wb' )
					
					ot <- read.fwf( tf , c( 5 , 2 , 2 , 1 ) )
					
					ace <- "http://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2016/cps-redesign/asec16_currcov_extract.dat"
				
					cachaca( ace , tf , mode = 'wb' )
					
					ac <- read.fwf( tf , c( 5 , 2 , 1 ) ) 

					cachaca( "http://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2014/cps-redesign/pubuse_esioffer_2016.sas7bdat" , tf , mode = 'wb' )
					
					offer <- data.frame( haven::read_sas( tf ) )
				
				}
				
				names( ot ) <- c( 'ph_seq' , 'ppposold' , 'outtyp' , 'i_outtyp' )
				
				names( ac ) <- c( 'ph_seq' , 'ppposold' , 'census_anycov' )
				
				names( offer ) <- tolower( names( offer ) )
				
				ac[ ac$census_anycov == 2 , 'census_anycov' ] <- 0
				
				ot_ac <- merge( ot , ac )
				
				ot_ac_of <- merge( ot_ac , offer , by.x = c( 'ph_seq' , 'ppposold' ) , by.y = c( 'h_seq' , 'ppposold' ) )
				
				DBI::dbWriteTable( db , 'ot_ac_of' , ot_ac_of )
				
				rm( ot , ac , ot_ac , ot_ac_of ) ; gc()
				
				
				mmf <- DBI::dbListFields( db , 'ot_ac_of' )[ !( DBI::dbListFields( db , 'ot_ac_of' ) %in% DBI::dbListFields( db , 'hfp_pac' ) ) ]
				DBI::dbSendQuery( 
					db , 
					paste( 
						"create table hfp as select a.* , " , 
						paste( "b." , mmf , sep = "" , collapse = "," ) , 
						" from hfp_pac as a inner join ot_ac_of as b on a.h_seq = b.ph_seq AND a.ppposold = b.ppposold" 
					)
				)
				
				stopifnot( DBI::dbGetQuery( db , 'select count(*) from hfp' )[ 1 , 1 ] == DBI::dbGetQuery( db , 'select count(*) from hfp_pac' )[ 1 , 1 ] )
				
				DBI::dbRemoveTable( db , 'ot_ac_of' )
				
				DBI::dbRemoveTable( db , 'hfp_pac' )
				
				file.remove( tf )
			
			} else {
			
				DBI::dbSendQuery( db , "create table hfp as select * from hfpz" )
				
				DBI::dbRemoveTable( db , 'hfpz' )
				
			}
			
			

			if( catalog[ i , 'year' ] > 2004 ){
				

				# confirm that the number of records in the 2011 cps asec merged file
				# matches the number of records in the person file
				stopifnot( DBI::dbGetQuery( db , "select count(*) as count from hfp" ) == DBI::dbGetQuery( db , "select count(*) as count from person" ) )


				# # # # # # # # # # # # # # # # # #
				# load the replicate weight file  #
				# # # # # # # # # # # # # # # # # #
						
				# this process is also slow.
				# the CPS ASEC 2011 replicate weight file has 204,983 person-records.

				# census.gov website containing the current population survey's replicate weights file
				CPS.replicate.weight.file.location <- 
					ifelse(
						catalog[ i , 'year' ] == 2014.38 ,
						"http://thedataweb.rm.census.gov/pub/cps/march/CPS_ASEC_ASCII_REPWGT_2014_3x8_run5.zip" ,
						ifelse(
							catalog[ i , 'year' ] == 2014 ,
							"http://www.census.gov/housing/extract_files/weights/CPS_ASEC_ASCII_REPWGT_2014_FULLSAMPLE.DAT" ,
							paste0( 
								"http://thedataweb.rm.census.gov/pub/cps/march/CPS_ASEC_ASCII_REPWGT_" , 
								substr( catalog[ i , 'year' ] , 1 , 4 ) , 
								".zip" 
							)
						)
					)
					
				# census.gov website containing the current population survey's SAS import instructions
				if( catalog[ i , 'year' ] %in% 2014.38 ){
				
					CPS.replicate.weight.SAS.read.in.instructions <- tempfile()

					writeLines(
						paste(
							"INPUT" ,
							paste0( "pwwgt" , 0:160 , " " , seq( 1 , 1601 , 10 ) , "-" , seq( 10 , 1610 , 10 ) , " 0.4" , collapse = "\n" ) ,
							paste( "h_seq 1611 - 1615" , "pppos 1616-1617" , ";" , collapse = "\n" ) ,
							sep = "\n"
						) , 
						CPS.replicate.weight.SAS.read.in.instructions 
					)
					
				} else {
					
					CPS.replicate.weight.SAS.read.in.instructions <- 
						paste0( 
							"http://thedataweb.rm.census.gov/pub/cps/march/CPS_ASEC_ASCII_REPWGT_" , 
							substr( catalog[ i , 'year' ] , 1 , 4 ) , 
							".SAS" 
						)

				}

				zip_file <- 
					tolower( 
						substr( 
							CPS.replicate.weight.file.location , 
							nchar( CPS.replicate.weight.file.location ) - 2 , 
							nchar( CPS.replicate.weight.file.location ) 
						)
					) == 'zip'

					
				if( !zip_file ){
					rw_tf <- tempfile()
					cachaca( CPS.replicate.weight.file.location , rw_tf , mode = 'wb' )
					CPS.replicate.weight.file.location <- rw_tf
				}
				
				# store the CPS ASEC march 2011 replicate weight file as an R data frame
				read_SAScii_monetdb ( 
					CPS.replicate.weight.file.location , 
					CPS.replicate.weight.SAS.read.in.instructions , 
					zipped = zip_file , 
					tl = TRUE ,
					tablename = 'rw' ,
					connection = db
				)


				###################################################
				# merge cps asec file with replicate weights file #
				###################################################

				mmf <- DBI::dbListFields( db , 'rw' )[ !( DBI::dbListFields( db , 'rw' ) %in% DBI::dbListFields( db , 'hfp' ) ) ]
				
				sql <- paste( "create table" , catalog[ i , 'db_tablename' ] , "as select a.* , " , paste( "b." , mmf , sep = "" , collapse = "," ) , " from hfp as a inner join rw as b on a.h_seq = b.h_seq AND a.pppos = b.pppos" )
				
				DBI::dbSendQuery( db , sql )

			} else {
			
				mmf <- DBI::dbListFields( db , 'person' )[ !( DBI::dbListFields( db , 'person' ) %in% DBI::dbListFields( db , 'h_f_xwalk' ) ) ]
				
				sql <- paste( "create table" , catalog[ i , 'db_tablename' ] , "as select a.* , " , paste( "b." , mmf , sep = "" , collapse = "," ) , " from h_f_xwalk as a inner join person as b on a.h_seq = b.ph_seq AND a.pppos = b.pppos" )
				
				DBI::dbSendQuery( db , sql )
					
			}
				
			# confirm that the number of records in the 2011 person file
			# matches the number of records in the merged file
			stopifnot( DBI::dbGetQuery( db , paste( "select count(*) as count from " , catalog[ i , 'db_tablename' ] ) ) == DBI::dbGetQuery( db , "select count(*) as count from person" ) )

			# drop unnecessary tables
			try( DBI::dbSendQuery( db , "drop table h_xwalk" ) , silent = TRUE )
			try( DBI::dbSendQuery( db , "drop table h_f_xwalk" ) , silent = TRUE )
			try( DBI::dbSendQuery( db , "drop table xwalk" ) , silent = TRUE )
			DBI::dbSendQuery( db , "drop table hfp" )
			try( DBI::dbSendQuery( db , "drop table rw" ) , silent = TRUE )
			try( DBI::dbSendQuery( db , "drop table household" ) , silent = TRUE )
			try( DBI::dbSendQuery( db , "drop table hhld" ) , silent = TRUE )
			DBI::dbSendQuery( db , "drop table family" )
			DBI::dbSendQuery( db , "drop table person" )

			
			# add a new column "one" that simply contains the number 1 for every record in the data set
			DBI::dbSendQuery( db , paste( "ALTER TABLE" , catalog[ i , 'db_tablename' ] , "ADD one REAL" ) )
			DBI::dbSendQuery( db , paste( "UPDATE" , catalog[ i , 'db_tablename' ] , "SET one = 1" ) )

			# # # # # # # # # # # # # # # # # # # # # # # # # #
			# import the supplemental poverty research files  #
			# # # # # # # # # # # # # # # # # # # # # # # # # #
			
			overlapping.spm.fields <- c( "gestfips" , "fpovcut" , "ftotval" , "marsupwt" )
			
			if( catalog[ i , 'year' ] %in% c( 2010:2016 , 2014.38 , 2014.58 ) ){

				sp.url <- 
					paste0( 
					"http://www.census.gov/housing/povmeas/spmresearch/spmresearch" , 
					floor( catalog[ i , 'year' ] - 1 ) , 
					if ( catalog[ i , 'year' ] == 2014.38 ) "_redes" else if ( catalog[ i , 'year' ] >= 2014 ) "" else "new" ,
					".sas7bdat" 
				)
				
				cachaca( sp.url , tf , mode = 'wb' )
				
				sp <- haven::read_sas( tf )
			
				if ( catalog[ i , 'year' ] == 2014 ){
					
					sp.url <- "http://www.census.gov/housing/povmeas/spmresearch/spmresearch2013_redes.sas7bdat"
						
					cachaca( sp.url , tf , mode = 'wb' )
					
					sp2 <- haven::read_sas( tf )
				
					sp <- rbind( sp , sp2 )
					
					rm( sp2 ) ; gc()
					
				} 
				
				names( sp ) <- tolower( names( sp ) )

				sp <- sp[ , !( names( sp ) %in% overlapping.spm.fields ) ]

				DBI::dbWriteTable( db , paste0( catalog[ i , 'db_tablename' ] , "_sp" ) , sp )
				
				
				rm( sp ) ; gc()
			
				DBI::dbSendQuery( db , paste( 'create table temp as select * from' , catalog[ i , 'db_tablename' ] ) )
				
				DBI::dbRemoveTable( db , catalog[ i , 'db_tablename' ] )
				
				mmf <- DBI::dbListFields( db , paste0( catalog[ i , 'db_tablename' ] , '_sp' ) )[ !( DBI::dbListFields( db , paste0( catalog[ i , 'db_tablename' ] , '_sp' ) ) %in% DBI::dbListFields( db , 'temp' ) ) ]
				
				DBI::dbSendQuery( 
					db , 
					paste0( 
						"create table " , 
						catalog[ i , 'db_tablename' ] , 
						" as select a.* , " , paste( "b." , mmf , sep = "" , collapse = "," ) , " from temp as a inner join " ,
						catalog[ i , 'db_tablename' ] , 
						"_sp as b on a.h_seq = b.h_seq AND a.pppos = b.pppos" 
					) 
				)

				stopifnot( DBI::dbGetQuery( db , paste( "select count(*) as count from " , catalog[ i , 'db_tablename' ] ) ) == DBI::dbGetQuery( db , "select count(*) as count from temp" ) )
			
				DBI::dbRemoveTable( db , 'temp' )
						
			}
			
			
			# remove redundant colon fields
			ftk <- DBI::dbListFields( db , catalog[ i , 'db_tablename' ] )[ !grepl( ":" , DBI::dbListFields( db , catalog[ i , 'db_tablename' ] ) ) ]
			
			# copy over the fields-to-keep into a new table
			DBI::dbSendQuery( db , paste( "CREATE TABLE temp AS SELECT" , paste( ftk , collapse = "," ) , "FROM" , catalog[ i , 'db_tablename' ] ) )
			
			# drop the original
			DBI::dbSendQuery( db , paste( "DROP TABLE" , catalog[ i , 'db_tablename' ] ) )
			
			# rename the temporary table
			DBI::dbSendQuery( db , paste( "CREATE TABLE" , catalog[ i , 'db_tablename' ] , "AS SELECT * FROM temp WITH DATA" ) )
			DBI::dbRemoveTable( db , "temp" )
			
			# disconnect from the current monet database
			DBI::dbDisconnect( db , shutdown = TRUE )

			# delete the temporary files
			suppressWarnings( file.remove( tf ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'db_tablename' ] , "'\r\n\n" ) )

		}

		catalog

	}


	
	
# data dictionary parser
cps_asec_dd_parser <-
	function( url ){

		# read in the data dictionary
		lines <- readLines ( url )
		
		# find the record positions
		hh_start <- grep( "HOUSEHOLD RECORD" , lines )
		
		fm_start <- grep( "FAMILY RECORD" , lines )
		
		pn_start <- grep( "PERSON RECORD" , lines )
		
		# segment the data dictionary into three parts
		hh_lines <- lines[ hh_start:(fm_start - 1 ) ]
		
		fm_lines <- lines[ fm_start:( pn_start - 1 ) ]
		
		pn_lines <- lines[ pn_start:length(lines) ]
		
		# loop through all three parts		
		for ( i in c( "hh_lines" , "fm_lines" , "pn_lines" ) ){
		
			# pull the lines into a temporary variable
			k <- j <- get( i )
		
			# remove any goofy tab characters
			j <- gsub( "\t" , " " , j )
			
			# look for lines indicating divisor
			idp <- grep( "2 implied" , j )
			
			# confirm you've captured all decimal lines
			stopifnot( all( grep( "implied" , j ) == idp ) )
			
			# keep only the variable lines
			j <- grep( "^D " , j , value = TRUE )
			
			# remove all multiple-whitespaces
			while( any( grepl( "  " , j ) ) ) j <- gsub( "  " , " " , j )
			while( any( grepl( "  " , k ) ) ) k <- gsub( "  " , " " , k )
		
			# get rid of the prefix "D "
			j <- gsub( "^D " , "" , j )
			
			# get rid of any spaces at the end of each line
			j <- gsub( " $" , "" , j )
			
			# keep only the first three items in the line
			j <- gsub( "(.*) (.*) (.*) (.*)" , "\\1 \\2 \\3" , j )
		
			# break the lines apart by spacing
			j <- strsplit( j , " " )
			
			# store the variable name, width, and position into a data.frame
			j <-
				data.frame( 
					varname = sapply( j , '[[' , 1 ) ,
					width = as.numeric( sapply( j , '[[' , 2 ) ) ,
					position = as.numeric( sapply( j , '[[' , 3 ) ) , 
					divisor = 1
				)
		
			# confirm the cumulative sum of the widths equals the final position
			stopifnot( cumsum( j$width )[ nrow( j ) - 1 ] == j[ nrow( j ) , 'position' ] - 1 )

			# confirm that the last variable is filler and can be tossed
			if ( !grepl( "2015" , url ) ){
			
				stopifnot( j[ nrow( j ) , 'varname' ] == 'FILLER' )
			
				# toss it.
				j <- j[ -nrow( j ) , ]
				
			}
				
			# find the position of each variable name in the original file
			pos <- lapply( paste0( "^D " , j[ , 'varname' ] , " " ) , grep , k )
			
			# confirm all multiply-named columns are `FILLER`
			stopifnot( all( j[ lapply( pos , length ) != 1 , 'varname' ] == 'FILLER' ) )

			# add on the positions from the original file
			j$location_in_original_file <- unlist( lapply( pos , min ) )

			# everywhere with divisor, find the associated variable
			for ( l in idp ){
			
				which_dec <- max( j[ j$location_in_original_file < l , 'location_in_original_file' ] ) 
			
				j[ which_dec == j$location_in_original_file , 'divisor' ] <- 0.01

			}

			# remove that column you no longer need
			j$location_in_original_file <- NULL
		
			# overwrite - with _
			j$varname <- gsub( "-" , "_" , j$varname )

			# fillers should be missings not 
			j[ j$varname == 'FILLER' , 'width' ] <- -( j[ j$varname == 'FILLER' , 'width' ] )
			j[ j$varname == 'FILLER' , 'varname' ] <- NA
			
			
			# treat cps fields as exclusively numeric
			j$char <- FALSE
			
			assign( gsub( "_lines" , "_stru" , i ) , j )
			
		}
		
		
		mget( c( "hh_stru" , "fm_stru" , "pn_stru" ) )
		
	}
	
