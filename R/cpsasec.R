get_catalog_cpsasec <-
	function( data_name = "cpsasec" , output_dir , ... ){

		cps_ftp <- "https://thedataweb.rm.census.gov/ftp/cps_ftp.html#cpsmarch"

		cps_links <- rvest::html_attr( rvest::html_nodes( xml2::read_html( cps_ftp ) , "a" ) , "href" )
		
		these_links <- grep( "asec(.*)zip$" , cps_links , value = TRUE , ignore.case = TRUE )

		asec_max_year <- max( as.numeric( substr( gsub( "[^0-9]" , "" , these_links ) , 1 , 4 ) ) )
		
		asec_years <- c( 1998:2014 , 2014.58 , 2014.38 , seq( 2015 , asec_max_year ) )
		
		catalog <-
			data.frame(
				production_file = c( rep( TRUE , length( asec_years ) ) , FALSE , FALSE ) ,
				year = c( asec_years , 2017 , 2018 ) ,
				output_filename = file.path( output_dir , paste0( c( asec_years , '2017 research' , '2018 bridge' ) , " cps asec.rds" ) ) ,
				stringsAsFactors = FALSE
			)

		# overwrite 2014.38 with three-eights
		catalog$output_filename <- gsub( "\\.38" , "_3x8" , catalog$output_filename )

		# overwrite 2014.58 with three-eights
		catalog$output_filename <- gsub( "\\.58" , "_5x8" , catalog$output_filename )

		catalog <- catalog[ order( catalog[ , 'year' ] ) , ]
		
		catalog

	}


lodown_cpsasec <-
	function( data_name = "cpsasec" , catalog , ... ){

		on.exit( print( catalog ) )

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){
					
			# # # # # # # # # # # #
			# load the main file  #
			# # # # # # # # # # # #

			# this process is slow.
			# for example, the CPS ASEC 2011 file has 204,983 person-records.

			if( catalog[ i , 'year' ] >= 2019 ){
				
				td <- tempdir()
				
				cachaca( paste0( "https://thedataweb.rm.census.gov/pub/cps/march/asecpub" , substr( catalog[ i , 'year' ] , 3 , 4 ) , "sas.zip" ) , tf , mode = 'wb' , filesize_fun = 'unzip_verify' )

				asec_files <- unzip( tf , exdir = td )
				
				prsn <- data.frame( haven::read_sas( grep( 'pppub' , asec_files , value = TRUE ) ) )
				fmly <- data.frame( haven::read_sas( grep( 'ffpub' , asec_files , value = TRUE ) ) )
				hhld <- data.frame( haven::read_sas( grep( 'hhpub' , asec_files , value = TRUE ) ) )
				
				names( fmly ) <- tolower( names( fmly ) )
				for ( j in names( fmly ) ) fmly[ , j ] <- as.numeric( fmly[ , j ] )
				fmly$fsup_wgt <- fmly$fsup_wgt / 100
				
				number_of_records <- nrow( prsn )
				names( prsn ) <- tolower( names( prsn ) )
				for ( j in names( prsn ) ) prsn[ , j ] <- as.numeric( prsn[ , j ] )
				for ( j in c( 'marsupwt' , 'a_ernlwt' , 'a_fnlwgt' ) ) prsn[ , j ] <- prsn[ , j ] / 100
				names( fmly )[ names( fmly ) == 'fh_seq' ] <- 'h_seq'
				names( prsn )[ names( prsn ) == 'ph_seq' ] <- 'h_seq'
				names( prsn )[ names( prsn ) == 'phf_seq' ] <- 'ffpos'
				x <- merge( fmly , prsn )
				rm( fmly , prsn )

				names( hhld ) <- tolower( names( hhld ) )
				for ( j in setdiff( names( hhld ) , 'h_idnum' ) ) hhld[ , j ] <- as.numeric( hhld[ , j ] )
				hhld$hsup_wgt <- hhld$hsup_wgt / 100
				x <- merge( hhld , x )
				rm( hhld )
				
				names( x ) <- toupper( names( x ) )
				
				stopifnot( nrow( x ) == number_of_records )

				file.remove( asec_files , tf )


			} else if( !( catalog[ i , 'production_file' ] ) & ( catalog[ i , 'year' ] == 2017 ) ){

				tf1 <- tempfile() ; tf2 <- tempfile() ; tf3 <- tempfile()
			
				cachaca( "https://www2.census.gov/programs-surveys/demo/datasets/income-poverty/time-series/data-extracts/2017/cps-asec-research-file/hhpub17_010919.sas7bdat" , tf1 , mode = 'wb' , filesize_fun = 'sas_verify' )
				cachaca( "https://www2.census.gov/programs-surveys/demo/datasets/income-poverty/time-series/data-extracts/2017/cps-asec-research-file/ffpub17_010919.sas7bdat" , tf2 , mode = 'wb' , filesize_fun = 'sas_verify' )
				cachaca( "https://www2.census.gov/programs-surveys/demo/datasets/income-poverty/time-series/data-extracts/2017/cps-asec-research-file/pppub17.sas7bdat" , tf3 , mode = 'wb' , filesize_fun = 'sas_verify' )

				fmly <- data.frame( haven::read_sas( tf2 ) )
				names( fmly ) <- tolower( names( fmly ) )
				for ( j in names( fmly ) ) fmly[ , j ] <- as.numeric( fmly[ , j ] )
				fmly$fsup_wgt <- fmly$fsup_wgt / 100
				file.remove( tf2 )
				
				prsn <- data.frame( haven::read_sas( tf3 ) )
				number_of_records <- nrow( prsn )
				names( prsn ) <- tolower( names( prsn ) )
				for ( j in names( prsn ) ) prsn[ , j ] <- as.numeric( prsn[ , j ] )
				for ( j in c( 'marsupwt' , 'a_ernlwt' , 'a_fnlwgt' ) ) prsn[ , j ] <- prsn[ , j ] / 100
				names( fmly )[ names( fmly ) == 'fh_seq' ] <- 'h_seq'
				names( prsn )[ names( prsn ) == 'ph_seq' ] <- 'h_seq'
				names( prsn )[ names( prsn ) == 'phf_seq' ] <- 'ffpos'
				x <- merge( fmly , prsn )
				rm( fmly , prsn ) ; gc() ; file.remove( tf3 )

				hhld <- data.frame( haven::read_sas( tf1 ) )
				names( hhld ) <- tolower( names( hhld ) )
				for ( j in names( hhld ) ) hhld[ , j ] <- as.numeric( hhld[ , j ] )
				hhld$hsup_wgt <- hhld$hsup_wgt / 100
				x <- merge( hhld , x )
				rm( hhld ) ; gc() ; file.remove( tf1 )
				
				names( x ) <- toupper( names( x ) )
				
				stopifnot( nrow( x ) == number_of_records )

				
			} else if( !( catalog[ i , 'production_file' ] ) & ( catalog[ i , 'year' ] == 2018 ) ){

				tf1 <- tempfile() ; tf2 <- tempfile() ; tf3 <- tempfile()
			
				cachaca( "https://www2.census.gov/programs-surveys/demo/datasets/income-poverty/time-series/data-extracts/2018/cps-asec-bridge-file/hhpub18_bridge.sas7bdat" , tf1 , mode = 'wb' , filesize_fun = 'sas_verify' )
				cachaca( "https://www2.census.gov/programs-surveys/demo/datasets/income-poverty/time-series/data-extracts/2018/cps-asec-bridge-file/ffpub18_bridge.sas7bdat" , tf2 , mode = 'wb' , filesize_fun = 'sas_verify' )
				cachaca( "https://www2.census.gov/programs-surveys/demo/datasets/income-poverty/time-series/data-extracts/2018/cps-asec-bridge-file/ppub18_bridge.sas7bdat" , tf3 , mode = 'wb' , filesize_fun = 'sas_verify' )

				
				fmly <- data.frame( haven::read_sas( tf2 ) )
				names( fmly ) <- tolower( names( fmly ) )
				for ( j in names( fmly ) ) fmly[ , j ] <- as.numeric( fmly[ , j ] )
				fmly$fsup_wgt <- fmly$fsup_wgt / 100
				file.remove( tf2 )
				
				prsn <- data.frame( haven::read_sas( tf3 ) )
				number_of_records <- nrow( prsn )
				names( prsn ) <- tolower( names( prsn ) )
				for ( j in names( prsn ) ) prsn[ , j ] <- as.numeric( prsn[ , j ] )
				for ( j in c( 'marsupwt' , 'a_ernlwt' , 'a_fnlwgt' ) ) prsn[ , j ] <- prsn[ , j ] / 100
				names( fmly )[ names( fmly ) == 'fh_seq' ] <- 'h_seq'
				names( prsn )[ names( prsn ) == 'ph_seq' ] <- 'h_seq'
				names( prsn )[ names( prsn ) == 'phf_seq' ] <- 'ffpos'
				x <- merge( fmly , prsn )
				rm( fmly , prsn ) ; gc() ; file.remove( tf3 )

				hhld <- data.frame( haven::read_sas( tf1 ) )
				names( hhld ) <- tolower( names( hhld ) )
				for ( j in names( hhld ) ) hhld[ , j ] <- as.numeric( hhld[ , j ] )
				hhld$hsup_wgt <- hhld$hsup_wgt / 100
				x <- merge( hhld , x )
				rm( hhld ) ; gc() ; file.remove( tf1 )
				
				names( x ) <- toupper( names( x ) )
				
				stopifnot( nrow( x ) == number_of_records )


			# for the 2014 cps, load the income-consistent file as the full-catalog[ i , 'year' ] extract
			} else if ( catalog[ i , 'year' ] == 2014 ){
				
				tf1 <- tempfile() ; tf2 <- tempfile() ; tf3 <- tempfile()
			
				cachaca( "https://www2.census.gov/programs-surveys/demo/datasets/income-poverty/time-series/data-extracts/hhld.sas7bdat" , tf1 , mode = 'wb' , filesize_fun = 'sas_verify' )
				cachaca( "https://www2.census.gov/programs-surveys/demo/datasets/income-poverty/time-series/data-extracts/family.sas7bdat" , tf2 , mode = 'wb' , filesize_fun = 'sas_verify' )
				cachaca( "https://www2.census.gov/programs-surveys/demo/datasets/income-poverty/time-series/data-extracts/person.sas7bdat" , tf3 , mode = 'wb' , filesize_fun = 'sas_verify' )

				
				fmly <- data.frame( haven::read_sas( tf2 ) )
				names( fmly ) <- tolower( names( fmly ) )
				for ( j in names( fmly ) ) fmly[ , j ] <- as.numeric( fmly[ , j ] )
				fmly$fsup_wgt <- fmly$fsup_wgt / 100
				file.remove( tf2 )
				
				prsn <- data.frame( haven::read_sas( tf3 ) )
				number_of_records <- nrow( prsn )
				names( prsn ) <- tolower( names( prsn ) )
				for ( j in names( prsn ) ) prsn[ , j ] <- as.numeric( prsn[ , j ] )
				for ( j in c( 'marsupwt' , 'a_ernlwt' , 'a_fnlwgt' ) ) prsn[ , j ] <- prsn[ , j ] / 100
				names( fmly )[ names( fmly ) == 'fh_seq' ] <- 'h_seq'
				names( prsn )[ names( prsn ) == 'ph_seq' ] <- 'h_seq'
				names( prsn )[ names( prsn ) == 'phf_seq' ] <- 'ffpos'
				x <- merge( fmly , prsn )
				rm( fmly , prsn ) ; gc() ; file.remove( tf3 )

				hhld <- data.frame( haven::read_sas( tf1 ) )
				names( hhld ) <- tolower( names( hhld ) )
				for ( j in names( hhld ) ) hhld[ , j ] <- as.numeric( hhld[ , j ] )
				hhld$hsup_wgt <- hhld$hsup_wgt / 100
				x <- merge( hhld , x )
				rm( hhld ) ; gc() ; file.remove( tf1 )
				
				names( x ) <- toupper( names( x ) )
				
				stopifnot( nrow( x ) == number_of_records )

			} else {
				
				# note: this CPS March Supplement ASCII (fixed-width file) contains household-, family-, and person-level records.

				# census.gov website containing the current population survey's main file
				CPS.ASEC.mar.file.location <- 
					ifelse( 
						# if the catalog[ i , 'year' ] to download is 2007, the filename doesn't match the others..
						catalog[ i , 'year' ] == 2007 ,
						"https://thedataweb.rm.census.gov/pub/cps/march/asec2007_pubuse_tax2.zip" ,
						ifelse(
							catalog[ i , 'year' ] %in% 2004:2003 ,
							paste0( "https://thedataweb.rm.census.gov/pub/cps/march/asec" , catalog[ i , 'year' ] , ".zip" ) ,
							ifelse(
								catalog[ i , 'year' ] %in% 2002:1998 ,
								paste0( "https://thedataweb.rm.census.gov/pub/cps/march/mar" , substr( catalog[ i , 'year' ] , 3 , 4 ) , "supp.zip" ) ,
								ifelse( 
									catalog[ i , 'year' ] == 2014.38 ,
									"https://thedataweb.rm.census.gov/pub/cps/march/asec2014_pubuse_3x8_rerun_v2.zip" ,
									ifelse( 
										catalog[ i , 'year' ] == 2014.58 ,
										"https://thedataweb.rm.census.gov/pub/cps/march/asec2014_pubuse_tax_fix_5x8_2017.zip" ,
										ifelse( catalog[ i , 'year' ] == 2016 ,
											paste0( "https://thedataweb.rm.census.gov/pub/cps/march/asec" , catalog[ i , 'year' ] , "_pubuse_v3.zip" ) ,
											# ifelse( catalog[ i , 'year' ] == 2018 ,
												# "https://thedataweb.rm.census.gov/pub/cps/march/asec2018early_pubuse.zip" ,
												paste0( "https://thedataweb.rm.census.gov/pub/cps/march/asec" , catalog[ i , 'year' ] , "_pubuse.zip" )
											# )
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
					
					if( catalog[ i , 'year' ] >= 2017 ) sas_ris <- cpsasec_dd_parser( paste0( "https://thedataweb.rm.census.gov/pub/cps/march/08ASEC" , catalog[ i , 'year' ] , "_Data_Dict_Full.txt" ) )
					if( catalog[ i , 'year' ] == 2016 ) sas_ris <- cpsasec_dd_parser( paste0( "https://thedataweb.rm.census.gov/pub/cps/march/Asec2016_Data_Dict_Full.txt" ) )
					if( catalog[ i , 'year' ] == 2015 ) sas_ris <- cpsasec_dd_parser( "https://thedataweb.rm.census.gov/pub/cps/march/asec2015early_pubuse.dd.txt" )
					if( catalog[ i , 'year' ] == 2014.38 ) sas_ris <- cpsasec_dd_parser( "https://thedataweb.rm.census.gov/pub/cps/march/asec2014R_pubuse.dd.txt" )
					if( catalog[ i , 'year' ] == 2014.58 ) sas_ris <- cpsasec_dd_parser( "https://thedataweb.rm.census.gov/pub/cps/march/asec2014early_pubuse.dd.txt" )
					if( catalog[ i , 'year' ] == 2013 ) sas_ris <- cpsasec_dd_parser( "https://thedataweb.rm.census.gov/pub/cps/march/asec2013early_pubuse.dd.txt" )
					if( catalog[ i , 'year' ] == 2012 ) sas_ris <- cpsasec_dd_parser( "https://thedataweb.rm.census.gov/pub/cps/march/asec2012early_pubuse.dd.txt" )
					if( catalog[ i , 'year' ] == 2011 ) sas_ris <- cpsasec_dd_parser( "https://thedataweb.rm.census.gov/pub/cps/march/asec2011_pubuse.dd.txt" )

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

				# one read-only file connection "rb" - pointing to the ASCII file
				incon <- file( fn , "rb") 

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

				number_of_records <- R.utils::countLines( tf.xwalk )

				# the 2011 SAS file produced by the National Bureau of Economic Research (NBER)
				# begins each INPUT block after lines 988, 1121, and 1209, 
				# so skip SAS import instruction lines before that.
				# NOTE that this 'beginline' parameters of 988, 1121, and 1209 will change for different years.

				if( catalog[ i , 'year' ] < 2011 ) {
					hhld <-
						read_SAScii(
							tf.household , 
							CPS.ASEC.mar.SAS.read.in.instructions , 
							beginline = hh_beginline , 
							zipped = FALSE
						)
					
					fmly <-
						read_SAScii(
							tf.family , 
							CPS.ASEC.mar.SAS.read.in.instructions , 
							beginline = fa_beginline , 
							zipped = FALSE
						)
					
					prsn <-
						read_SAScii(
							tf.person , 
							CPS.ASEC.mar.SAS.read.in.instructions , 
							beginline = pe_beginline , 
							zipped = FALSE
						)
					
				} else {
				
					hhld <-
						read_SAScii(
							tf.household , 
							sas_stru = sas_ris[[1]] , 
							beginline = hh_beginline , 
							zipped = FALSE
						)
					
					fmly <-
						read_SAScii(
							tf.family , 
							sas_stru = sas_ris[[2]]  , 
							beginline = fa_beginline , 
							zipped = FALSE
						)
					
					prsn <-
						read_SAScii(
							tf.person , 
							sas_stru = sas_ris[[3]]  , 
							beginline = pe_beginline , 
							zipped = FALSE
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

				
				xwalk <-
					read_SAScii ( 
						tf.xwalk , 
						xwalk.sas.tf , 
						zipped = FALSE
					)
				
				x <- merge( xwalk , hhld ) ; rm( xwalk , hhld ) ; gc()
				names( fmly )[ names( fmly ) == 'FH_SEQ' ] <- 'H_SEQ'
				x <- merge( x , fmly ) ; rm( fmly ) ; gc()
				names( prsn )[ names( prsn ) == 'PH_SEQ' ] <- 'H_SEQ'
				x <- merge( x , prsn )
				
				stopifnot( nrow( x ) == number_of_records )
			
			}
				
			# tack on _anycov_ variables
			# tack on _outtyp_ variables
			if( ( catalog[ i , 'production_file' ] ) & ( catalog[ i , 'year' ] %in% c( 2018 , 2017 , 2016 , 2015 , 2014.58 , 2014.38 , 2014 ) ) ){
				
				tf <- tempfile()
				
				ac <- NULL
				
				ot <- NULL
				
				if( catalog[ i , 'year' ] %in% c( 2014 , 2014.58 ) ){
					
					ace <- "https://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2014/cps-redesign/asec14_now_anycov.dat"

					download.file( ace , tf , mode = "wb" )	

					ac <- rbind( ac , data.frame( readr::read_fwf( tf , readr::fwf_widths( c( 5 , 2 , 1 ) ) , col_types = 'nnn' ) ) )
					
				}

				if( catalog[ i , 'year' ] %in% c( 2014 , 2014.38 ) ){
					
					ace <- "https://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2014/cps-redesign/asec14_now_anycov_redes.dat"

					download.file( ace , tf , mode = "wb" )	

					ac <- rbind( ac , data.frame( readr::read_fwf( tf , readr::fwf_widths( c( 5 , 2 , 1 ) ) , col_types = 'nnn' ) ) )
					
				}
				
				if ( catalog[ i , 'year' ] %in% c( 2014 , 2014.38 , 2014.58 ) ){
				
					ote <- "https://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2014/cps-redesign/asec14_outtyp_full.dat"
					
					download.file( ote , tf , mode = 'wb' )
					
					ot <- data.frame( readr::read_fwf( tf , readr::fwf_widths( c( 5 , 2 , 2 , 1 ) ) , col_types = 'nnnn' ) )
					
					cachaca( "https://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2014/cps-redesign/ppint14esi_offer_ext.sas7bdat" , tf , mode = 'wb' , filesize_fun = 'sas_verify' )
					
					offer <- data.frame( haven::read_sas( tf ) )
					
				}
				
				
				if ( catalog[ i , 'year' ] %in% 2015 ){
				
					ote <- "https://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2014/cps-redesign/asec15_outtyp.dat"
				
					download.file( ote , tf , mode = 'wb' )
					
					ot <- data.frame( readr::read_fwf( tf , readr::fwf_widths( c( 5 , 2 , 2 , 1 ) ) , col_types = 'nnnn' ) )
					
					ace <- "https://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2014/cps-redesign/asec15_currcov_extract.dat"
				
					download.file( ace , tf , mode = 'wb' )
					
					ac <- data.frame( readr::read_fwf( tf , readr::fwf_widths( c( 5 , 2 , 1 ) ) , col_types = 'nnn' ) )

					cachaca( "https://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2014/cps-redesign/ppint15esi_offer_ext.sas7bdat" , tf , mode = 'wb' , filesize_fun = 'sas_verify' )
					
					offer <- data.frame( haven::read_sas( tf ) )
				
				}
				
				if ( catalog[ i , 'year' ] %in% 2016 ){
				
					ote <- "https://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2016/cps-redesign/asec16_outtyp_full.dat"
				
					download.file( ote , tf , mode = 'wb' )
					
					ot <- data.frame( readr::read_fwf( tf , readr::fwf_widths( c( 5 , 2 , 2 , 1 ) ) , col_types = 'nnnn' ) )
					
					ace <- "https://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2016/cps-redesign/asec16_currcov_extract.dat"
				
					download.file( ace , tf , mode = 'wb' )
					
					ac <- data.frame( readr::read_fwf( tf , readr::fwf_widths( c( 5 , 2 , 1 ) ) , col_types = 'nnn' ) )

					cachaca( "https://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2014/cps-redesign/pubuse_esioffer_2016.sas7bdat" , tf , mode = 'wb' , filesize_fun = 'sas_verify' )
					
					offer <- data.frame( haven::read_sas( tf ) )
				
				}
				
				if ( catalog[ i , 'year' ] %in% 2017 ){
				
					ote <- "https://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2017/cps-redesign/asec17_outtyp_extract.dat"
				
					download.file( ote , tf , mode = 'wb' )
					
					ot <- data.frame( readr::read_fwf( tf , readr::fwf_widths( c( 5 , 2 , 2 , 1 ) ) , col_types = 'nnnn' ) )
					
					ace <- "https://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2017/cps-redesign/asec17_currcov_extract.dat"
				
					download.file( ace , tf , mode = 'wb' )
					
					ac <- data.frame( readr::read_fwf( tf , readr::fwf_widths( c( 5 , 2 , 1 ) ) , col_types = 'nnn' ) )

					cachaca( "https://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2017/cps-redesign/pubuse_esioffer_2017.sas7bdat" , tf , mode = 'wb' , filesize_fun = 'sas_verify' )
					
					offer <- data.frame( haven::read_sas( tf ) )
				
				}
				
				if ( catalog[ i , 'year' ] %in% 2018 ){
				
					ote <- "https://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2018/cps-redesign/asec18_outtyp_extract.dat"
				
					download.file( ote , tf , mode = 'wb' )
					
					ot <- data.frame( readr::read_fwf( tf , readr::fwf_widths( c( 5 , 2 , 2 , 1 ) ) , col_types = 'nnnn' ) )
					
					ace <- "https://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2018/cps-redesign/asec18_currcov_extract.dat"
				
					download.file( ace , tf , mode = 'wb' )
					
					ac <- data.frame( readr::read_fwf( tf , readr::fwf_widths( c( 5 , 2 , 1 ) ) , col_types = 'nnn' ) )

					cachaca( "https://www2.census.gov/programs-surveys/demo/datasets/health-insurance/2018/cps-redesign/pubuse_esioffer_2018.sas7bdat" , tf , mode = 'wb' , filesize_fun = 'sas_verify' )
					
					offer <- data.frame( haven::read_sas( tf ) )
				
				}
				names( ot ) <- c( 'ph_seq' , 'ppposold' , 'outtyp' , 'i_outtyp' )
				
				names( ac ) <- c( 'ph_seq' , 'ppposold' , 'census_anycov' )
				
				names( offer ) <- tolower( names( offer ) )
				
				ac[ ac$census_anycov == 2 , 'census_anycov' ] <- 0
				
				ot_ac <- merge( ot , ac )
				
				ot_ac_of <- merge( ot_ac , offer , by.x = c( 'ph_seq' , 'ppposold' ) , by.y = c( 'h_seq' , 'ppposold' ) )
				
				x <- merge( x , ot_ac_of , by.x = c( 'H_SEQ' , 'PPPOSOLD' ) , by.y = c( "ph_seq" , "ppposold" ) )
				
				rm( ot , ac , ot_ac , ot_ac_of ) ; gc()
				
				stopifnot( nrow( x ) == number_of_records )
				
				file.remove( tf )
			
			}
			
			

			if( catalog[ i , 'year' ] > 2004 ){

				# # # # # # # # # # # # # # # # # #
				# load the replicate weight file  #
				# # # # # # # # # # # # # # # # # #
						
				# this process is also slow.
				# the CPS ASEC 2011 replicate weight file has 204,983 person-records.

				# census.gov website containing the current population survey's replicate weights file
				CPS.replicate.weight.file.location <- 
					ifelse(
						!( catalog[ i , 'production_file' ] ) & catalog[ i , 'year' ] == 2017 ,
						"https://www2.census.gov/programs-surveys/demo/datasets/income-poverty/time-series/data-extracts/2017/cps-asec-research-file/repwgt_2017.sas7bdat" ,
						ifelse(
							!( catalog[ i , 'production_file' ] ) & catalog[ i , 'year' ] == 2018 ,
							"https://www2.census.gov/programs-surveys/demo/datasets/income-poverty/time-series/data-extracts/2018/cps-asec-bridge-file/repwgt_2018.sas7bdat" ,
							ifelse(
								catalog[ i , 'year' ] == 2014.38 ,
								"https://www2.census.gov/programs-surveys/demo/datasets/income-poverty/time-series/weights/cps-asec-ascii-repwgt-2014-redes.dat" ,
								ifelse(
									catalog[ i , 'year' ] == 2014 ,
									"https://www2.census.gov/programs-surveys/demo/datasets/income-poverty/time-series/weights/cps-asec-ascii-repwgt-2014-fullsample.dat" ,
									paste0( 
										"https://thedataweb.rm.census.gov/pub/cps/march/CPS_ASEC_ASCII_REPWGT_" , 
										substr( catalog[ i , 'year' ] , 1 , 4 ) , 
										".zip" 
									)
								)
							)
						)
					)

					
				# census.gov website containing the current population survey's SAS import instructions
				if( ( catalog[ i , 'year' ] %in% 2014.38 ) ){
				
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
					
				} else if( !( catalog[ i , 'production_file' ] ) ){
				
					CPS.replicate.weight.SAS.read.in.instructions <- NULL
				
				} else {
					
					CPS.replicate.weight.SAS.read.in.instructions <- 
						paste0( 
							"https://thedataweb.rm.census.gov/pub/cps/march/CPS_ASEC_ASCII_REPWGT_" , 
							substr( catalog[ i , 'year' ] , 1 , 4 ) , 
							".SAS" 
						)

				}


				if( !( catalog[ i , 'production_file' ] ) ){
					
					rw_tf <- tempfile()
					cachaca( CPS.replicate.weight.file.location , rw_tf , mode = 'wb' , filesize_fun = 'sas_verify' )
					rw <- data.frame( haven::read_sas( rw_tf ) )
					names( rw ) <- toupper( names( rw ) )
					rw[ grepl( "MARSUPWT" , names( rw ) ) ] <-
						sapply( rw[ grepl( "MARSUPWT" , names( rw ) ) ] , function( w ) w * 1000 )
						
					names( rw ) <- gsub( "MARSUPWT_" , "PWWGT" , names( rw ) )
				
				} else {
				
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
						download.file( CPS.replicate.weight.file.location , rw_tf , mode = 'wb' )
						CPS.replicate.weight.file.location <- rw_tf
					}
					
					# store the CPS ASEC march 2011 replicate weight file as an R data frame
					rw <-
						read_SAScii( 
							CPS.replicate.weight.file.location , 
							CPS.replicate.weight.SAS.read.in.instructions , 
							zipped = zip_file
						)

				}
				
				
				###################################################
				# merge cps asec file with replicate weights file #
				###################################################

				x <- merge( x , rw ) ; rm( rw ) ; gc()
				
				stopifnot( nrow( x ) == number_of_records )
				
			}
			
			
			x$one <- 1
				
			# # # # # # # # # # # # # # # # # # # # # # # # # #
			# import the supplemental poverty research files  #
			# # # # # # # # # # # # # # # # # # # # # # # # # #
			
			overlapping.spm.fields <- c( "gestfips" , "fpovcut" , "ftotval" , "marsupwt" )
			
			if( ( catalog[ i , 'production_file' ] ) & ( catalog[ i , 'year' ] %in% c( 2010:2018 , 2014.38 , 2014.58 ) ) ){

				if( catalog[ i , 'year' ] >= 2017 ){
				
					sp.url <-
						paste0(
							"https://www2.census.gov/programs-surveys/supplemental-poverty-measure/datasets/spm/spmresearch" , 
							catalog[ i , 'year' ] - 1 , 
							".sas7bdat"
						)
						
				} else {
				
					sp.url <- 
						paste0( 
							"https://www2.census.gov/programs-surveys/supplemental-poverty-measure/datasets/spm" , 
							if( catalog[ i , 'year' ] == 2014.38 ) "-redes" , 
							"/spmresearch" , 
							floor( catalog[ i , 'year' ] - 1 ) , 
							if ( catalog[ i , 'year' ] == 2014.38 ) "_redes" else if ( catalog[ i , 'year' ] >= 2015 ) "" else "new" ,
							".sas7bdat" 
						)
				
				}
				
				cachaca( sp.url , tf , mode = 'wb' , filesize_fun = 'sas_verify' )
				
				sp <- data.frame( haven::read_sas( tf ) )
			
				if ( catalog[ i , 'year' ] == 2014 ){
					
					sp.url <- "https://www2.census.gov/programs-surveys/supplemental-poverty-measure/datasets/spm-redes/spmresearch2013_redes.sas7bdat"
						
					cachaca( sp.url , tf , mode = 'wb' , filesize_fun = 'sas_verify' )
					
					sp2 <- data.frame( haven::read_sas( tf ) )
				
					names( sp ) <- toupper( names( sp ) )
					names( sp2 ) <- toupper( names( sp2 ) )
					sp <- sp[ intersect( names( sp ) , names( sp2 ) ) ]
					sp2 <- sp2[ intersect( names( sp ) , names( sp2 ) ) ]
					
					sp <- rbind( sp , sp2 )
					
					rm( sp2 ) ; gc()
					
				} 
				
				names( sp ) <- toupper( names( sp ) )
				
				non_merge_cols <- setdiff( intersect( names( x ) , names( sp ) ) , c( 'H_SEQ' , 'PPPOS' ) )
				
				sp <- sp[ !( names( sp ) %in% non_merge_cols ) ]
				
				x <- merge( x , sp ) ; rm( sp ) ; gc()
				
				
			}
			
			stopifnot( nrow( x ) == number_of_records )
			
			catalog[ i , 'case_count' ] <- nrow( x )
			
			names( x ) <- tolower( names( x ) )
			
			saveRDS( x , file = catalog[ i , 'output_filename' ] , compress = FALSE ) ; rm( x ) ; gc()
			
			# delete the temporary files
			suppressWarnings( file.remove( tf ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r" ) )

		}

		on.exit()
		
		catalog

	}


	
	
# data dictionary parser
cpsasec_dd_parser <-
	function( this_url ){

		tf <- tempfile()
	
		# read in the data dictionary
		httr::GET( this_url , httr::write_disk( tf , overwrite = TRUE ) )
		
		these_lines <- readLines ( tf )
		
		# find the record positions
		hh_start <- grep( "HOUSEHOLD RECORD" , these_lines )
		
		fm_start <- grep( "FAMILY RECORD" , these_lines )
		
		pn_start <- grep( "PERSON RECORD" , these_lines )
		
		# segment the data dictionary into three parts
		hh_lines <- these_lines[ hh_start:(fm_start - 1 ) ]
		
		fm_lines <- these_lines[ fm_start:( pn_start - 1 ) ]
		
		pn_lines <- these_lines[ pn_start:length(these_lines) ]
		
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
			if ( !grepl( "2015" , this_url ) ){
			
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
	
