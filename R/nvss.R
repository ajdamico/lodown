get_catalog_nvss <-
	function( data_name = "nvss" , output_dir , ... ){

		if ( !requireNamespace( "archive" , quietly = TRUE ) ) stop( "archive needed for this function to work. to install it, type `devtools::install_github( 'jimhester/archive' )`" , call. = FALSE )

		catalog <- NULL
			
		# create a character string containing the cdc's vital statistics website
		url.with.data <- "https://www.cdc.gov/nchs/data_access/vitalstatsonline.htm"

		# pull that html code directly into R
		z <- readLines( url.with.data )

		# get rid of all tab characters
		z <- gsub( "\t" , "" , z )

		# keep only the lines in the html code containing an ftp site
		files <- z[ grep( 'ftp://ftp.cdc.gov/pub/Health_Statistics/NCHS/' , z ) ]
		# this, i'm assuming, points to every file available for download.  cool.

		catalog <-
			rbind(
				data.frame( type = 'natality' , year = 1999:nvss_max_year( "/natality/" , files ) , stringsAsFactors = FALSE ) ,
				data.frame( type = 'periodlinked' , year = 2001:nvss_max_year( "/periodlinkedus/" , files ) , stringsAsFactors = FALSE ) ,
				data.frame( type = 'cohortlinked' , year = 1995:nvss_max_year( "/cohortlinkedus/" , files ) , stringsAsFactors = FALSE ) ,
				data.frame( type = 'mortality' , year = 2000:nvss_max_year( "/mortality/" , files ) , stringsAsFactors = FALSE ) ,
				data.frame( type = 'fetaldeath' , year = 2005:nvss_max_year( "/fetaldeathus/" , files ) , stringsAsFactors = FALSE ) 
			)
		
		catalog$dbfile <- paste0( output_dir , "/SQLite.db" )
		
		catalog$output_folder <- output_dir
		
		catalog

	}


lodown_nvss <-
	function( data_name = "nvss" , catalog , ... ){

		on.exit( print( catalog ) )

		# create winrar extraction directories
		unique_directories <- unique( paste0( catalog$output_folder , "/winrar" ) )

		for ( this_dir in unique_directories ){
			if( !dir.exists( this_dir ) ){
				tryCatch( { 
					dir.create( this_dir , recursive = TRUE , showWarnings = TRUE ) 
					} , 
					warning = function( w ) stop( "while creating directory " , this_dir , "\n" , conditionMessage( w ) ) 
				)
			}
		}
		

		# create a character string containing the cdc's vital statistics website
		url.with.data <- "https://www.cdc.gov/nchs/data_access/vitalstatsonline.htm"

		# pull that html code directly into R
		z <- readLines( url.with.data )

		# get rid of all tab characters
		z <- gsub( "\t" , "" , z )

		# keep only the lines in the html code containing an ftp site
		files <- z[ grep( 'ftp://ftp.cdc.gov/pub/Health_Statistics/NCHS/' , z ) ]
		# this, i'm assuming, points to every file available for download.  cool.

		for ( i in seq_len( nrow( catalog ) ) ){

					
			# open the connection to the monetdblite database
			db <- DBI::dbConnect( RSQLite::SQLite() , catalog[ i , 'dbfile' ] )

			tables_before <- DBI::dbListTables( db )
			
			# loop through every year specified by the user
			if( catalog[ i , 'type' ] == 'natality' ){

				# for the current year, use a custom-built `nchs_extract_files` function
				# to determine the ftp location of the current natality file you're workin' on.
				natality <- nchs_extract_files( files[ grep( catalog[ i , 'year' ] , files ) ] , 'natality' )
				
				# download the natality file to the local working directory
				nchs_download( natality , catalog[ i , 'output_folder' ] )

				# create a character vector containing all files in the current working directory
				all.files <- list.files( catalog[ i , 'output_folder' ] , recursive = T , full.names = TRUE )

				# remove pdfs from possible identifier files
				all.files <- all.files[ !grepl( "\\.pdf$" , tolower( all.files ) ) ]
				
				# extract the filepaths of the nationwide natality files
				natality.us <- all.files[ grep( 'natality/us/' , all.files ) ]
				
				# extract the filepaths of the natality files of the territories
				natality.ps <- all.files[ grep( 'natality/ps/' , all.files ) ]

				# throw out all non-digits to extract the year from the data file
				year.plus.data <- years <- gsub( "\\D" , "" , basename( natality.us ) )

				# the pre-2006 files have an extra "/data" in the filepath of the sas import script
				year.plus.data[ as.numeric( year.plus.data ) %in% 1991:2005 ] <- 
					paste( year.plus.data[ as.numeric( year.plus.data ) %in% 1991:2005 ] , 'data' , sep = '/' )
				
				# use a custom-built function to re-arrange the sas importation script
				# so all column positions are sorted based on their @ sign
				sas_ri <- 
					nchs_order_at_signs( 
						# build the full http:// location of the current sas importation script
						paste0( 
							"http://www.nber.org/natality/" , 
							year.plus.data ,
							"/natl" ,
							years ,
							".sas"
						)
					)
				
				# the 2004 data file has a bunch of blanks at the end.
				# the sas importation script does not mention these in the slightest.
				if ( catalog[ i , 'year' ] == 2004 ) sas_ri <- nchs_extend_frace( sas_ri )
				# add 'em in.
				
				# prepare the downloaded data-file and the sas importation script
				# for a read_SAScii_monetdb() call.  then - hello operator - make the call!
				nchs_import( 
					natality.us , 
					sas.scripts = sas_ri ,
					db = db ,
					force.length = ifelse( catalog[ i , 'year' ] == 2004 , 1500 , FALSE )
				)
				
				
				# use a custom-built function to re-arrange the sas importation script
				# so all column positions are sorted based on their @ sign
				sas_ri <-
					nchs_order_at_signs(
						# build the full http:// location of the current sas importation script
						paste0( 
							"http://www.nber.org/natality/" , 
							year.plus.data ,
							"/natlterr" ,
							years ,
							".sas"
						)
					)

				# the 2004 data file has a bunch of blanks at the end.
				# the sas importation script does not mention these in the slightest.
				if ( catalog[ i , 'year' ] == 2004 ) sas_ri <- nchs_extend_frace( sas_ri )
				# add 'em in.
					
				# prepare the downloaded data-file and the sas importation script
				# for a read_SAScii_monetdb() call.  then - hello operator - make the call!
				nchs_import( 
					natality.ps , 
					sas.scripts = sas_ri ,
					db
				)

				# delete all files in the "/natality/us" directory (the fifty states plus DC)
				file.remove( list.files( paste0( catalog[ i , 'output_folder' ] , '/natality/us/' ) , recursive = TRUE , full.names = TRUE ) )
				
				# delete all files in the "/natality/ps" directory (the territories)
				file.remove( list.files( paste0( catalog[ i , 'output_folder' ] , '/natality/ps/' ) , recursive = TRUE , full.names = TRUE ) )
				
			}


			# if any period-linked data sets are queued up to be downloaded..
			if ( sum( catalog$type == 'periodlinked' ) > 0 ){

				# point to the period-linked sas file stored on github
				sas_ri_linked <- system.file("extdata", "nvss/nchs_period_linked.sas", package = "lodown")
				
				# create a temporary file
				den.tf <- tempfile()

				# also read it into working memory
				pl.txt <- readLines( sas_ri_linked )

				# add a semicolon after the FLGND field in order to indicate
				# that's the end of the numerator
				pl.den <- gsub( "flgnd 1346" , "flgnd 1346 ;" , pl.txt )

				# export that revised sas script to the second temporary file
				writeLines( pl.den , den.tf )
				
				# point to the period-linked 2013 sas file stored on github
				sas_ri_13 <- system.file("extdata", "nvss/nchs_period_linked_2013.sas", package = "lodown")

				# create a temporary file
				den13.tf <- tempfile()

				# also read it into working memory
				pl13.txt <- readLines( sas_ri_13 )

				# add a semicolon after the FLGND field in order to indicate
				# that's the end of the numerator
				pl13.den <- gsub( "FLGND  868" , "FLGND  868;" , pl13.txt )

				# export that revised sas script to the second temporary file
				writeLines( pl13.den , den13.tf )
				
				# point to the period-linked 2003 sas file stored on github
				sas_ri_03 <- system.file("extdata", "nvss/nchs_period_linked_2003.sas", package = "lodown")

				# create a temporary file
				den03.tf <- tempfile()

				# also read it into working memory
				pl03.txt <- readLines( sas_ri_03 )

				# add a semicolon after the FLGND field in order to indicate
				# that's the end of the numerator
				pl03.den <- gsub( "FLGND 751" , "FLGND 751;" , pl03.txt )

				# export that revised sas script to the second temporary file
				writeLines( pl03.den , den03.tf )
				
			}


			# loop through every year specified by the user
			if( catalog[ i , 'type' ] == 'periodlinked' ){

				# for the current year, use a custom-built `nchs_extract_files` function
				# to determine the ftp location of the current period-linked file you're workin' on.
				period.linked <- nchs_extract_files( files[ grep( catalog[ i , 'year' ] , files ) ] , 'periodlinked' )
				
				# download the period-linked file to the local working directory
				nchs_download( period.linked , catalog[ i , 'output_folder' ] )

				# create a character vector containing all files in the current working directory
				all.files <- list.files( catalog[ i , 'output_folder' ] , recursive = TRUE , full.names = TRUE )
				
				# extract the filepaths of the nationwide period-linked unlinked files
				periodlinked.us.unl <- all.files[ grep( 'periodlinked/us/unl' , all.files ) ]
				
				# extract the filepaths of the period-linked unlinked files for territories
				periodlinked.ps.unl <- all.files[ grep( 'periodlinked/ps/unl' , all.files ) ]

				# extract the filepaths of the nationwide period-linked numerator files
				periodlinked.us.num <- all.files[ grep( 'periodlinked/us/num' , all.files ) ]
				
				# extract the filepaths of the period-linked numerator files for territories
				periodlinked.ps.num <- all.files[ grep( 'periodlinked/ps/num' , all.files ) ]

				# extract the filepaths of the nationwide period-linked denominator files
				periodlinked.us.den <- all.files[ grep( 'periodlinked/us/den' , all.files ) ]
				
				# extract the filepaths of the period-linked denominator files for territories
				periodlinked.ps.den <- all.files[ grep( 'periodlinked/ps/den' , all.files ) ]

				
				# if the year is 2014 and beyond..
				if ( catalog[ i , 'year' ] > 2013 ){
				
					# use the period-linked sas import script
					sas_ri <- sas_ri_linked
				
				# ..otherwise, if the year is 2004-2013..
				} else if ( catalog[ i , 'year' ] > 2003 ){
				
					# use the period-linked sas import script
					sas_ri <- sas_ri_13
				
				# ..otherwise, if the year is 2003..
				} else if ( catalog[ i , 'year' ] == 2003 ) {
					
					# use the 2003 period-linked sas import script
					sas_ri <- sas_ri_03
					
				# otherwise..
				} else {
				
					# if the year is pre-1999, use a capital letter in the filepath's crispity-crunchity center
					file.middle <- ifelse( catalog[ i , 'year' ] < 1999 , "/data/Num" , "/data/num" )
				
					# build the full sas filepath to the period-linked numerator
					sas_ri <- 
						paste0( 
							"http://www.nber.org/perinatal/" , 
							catalog[ i , 'year' ] ,
							file.middle ,
							substr( catalog[ i , 'year' ] , 3 , 4 ) ,
							".sas"
						)
						
				}	
				

				# prepare the downloaded data-file and the sas importation script
				# for a read_SAScii_monetdb() call.  then - hello operator - make the call!
				nchs_import( 
					periodlinked.us.num , 
					sas.scripts = sas_ri ,
					db = db
				)
				
				# prepare the downloaded data-file and the sas importation script
				# for a read_SAScii_monetdb() call.  then - hello operator - make the call!
				nchs_import( 
					periodlinked.ps.num , 
					sas.scripts = sas_ri ,
					db
				)

				
				# if the year is 2014 and beyond..
				if ( catalog[ i , 'year' ] > 2013 ){
				
					# use the period-linked sas import script
					sas_ri <- sas_ri_linked
				
				# ..otherwise, if the year is 2004-2013..
				} else if ( catalog[ i , 'year' ] > 2003 ){
				
					# use the period-linked sas import script
					sas_ri <- sas_ri_13
				
				# ..otherwise, if the year is 2003..
				} else if ( catalog[ i , 'year' ] == 2003 ) {
					
					# use the 2003 period-linked sas import script
					sas_ri <- sas_ri_03
					
				# otherwise..
				} else {
				
					# build the full sas filepath to the period-linked unlinked
					sas_ri <- 
						paste0( 
							"http://www.nber.org/perinatal/" , 
							catalog[ i , 'year' ] ,
							"/data/unl" ,
							substr( catalog[ i , 'year' ] , 3 , 4 ) ,
							".sas"
						)
						
				}

				# prepare the downloaded data-file and the sas importation script
				# for a read_SAScii_monetdb() call.  then - hello operator - make the call!
				nchs_import( 
					periodlinked.us.unl , 
					sas.scripts = sas_ri ,
					db = db
				)

				# prepare the downloaded data-file and the sas importation script
				# for a read_SAScii_monetdb() call.  then - hello operator - make the call!
				nchs_import( 
					periodlinked.ps.unl , 
					sas.scripts = sas_ri ,
					db = db
				)
				
				# if the year is 2014 and beyond..	
				if ( catalog[ i , 'year' ] > 2013 ){
					
					# use the period-linked sas import script
					sas_ri <- den.tf
				
				# ..otherwise, if the year is 2004-2013..
				} else if ( catalog[ i , 'year' ] > 2003 ){
					
					# use the period-linked sas import script
					sas_ri <- den13.tf
				
				# ..otherwise, if the year is 2003..
				} else if ( catalog[ i , 'year' ] == 2003 ) {
					
					# use the 2003 period-linked sas import script
					sas_ri <- den03.tf
					
				# otherwise..
				} else {
				
					# build the full sas filepath to the period-linked unlinked
					sas_ri <-
						paste0( 
							"http://www.nber.org/perinatal/" , 
							catalog[ i , 'year' ] ,
							"/data/den" ,
							substr( catalog[ i , 'year' ] , 3 , 4 ) ,
							".sas"
						)
						
				}

				# prepare the downloaded data-file and the sas importation script
				# for a read_SAScii_monetdb() call.  then - hello operator - make the call!
				nchs_import( 
					periodlinked.us.den , 
					sas.scripts = sas_ri ,
					db = db
				)

				# prepare the downloaded data-file and the sas importation script
				# for a read_SAScii_monetdb() call.  then - hello operator - make the call!
				nchs_import( 
					periodlinked.ps.den , 
					sas.scripts = sas_ri ,
					db = db
				)

				# delete all files in the "/periodlinked/us" directory (the fifty states plus DC)
				file.remove( list.files( paste0( catalog[ i , 'output_folder' ] , '/periodlinked/us/' ) , recursive = TRUE , full.names = TRUE ) )

				# delete all files in the "/periodlinked/ps" directory (the fifty states plus DC)
				file.remove( list.files( paste0( catalog[ i , 'output_folder' ] , '/periodlinked/ps/' ) , recursive = TRUE , full.names = TRUE ) )

			}


			# loop through every year specified by the user
			if( catalog[ i , 'type' ] == 'cohortlinked' ){

				# for the current year, use a custom-built `nchs_extract_files` function
				# to determine the ftp location of the current cohort-linked file you're workin' on.
				cohort.linked <- nchs_extract_files( files[ grep( catalog[ i , 'year' ] , files ) ]  , 'cohortlinked' )
				
				# download the cohort-linked file to the local working directory
				nchs_download( cohort.linked , catalog[ i , 'output_folder' ] )

				# create a character vector containing all files in the current working directory
				all.files <- list.files( catalog[ i , 'output_folder' ] , recursive = TRUE , full.names = TRUE )

				# remove pdfs from possible identifier files
				all.files <- all.files[ !grepl( "\\.pdf$" , tolower( all.files ) ) ]
				
				# extract the filepaths of the nationwide cohort-linked unlinked files
				cohortlinked.us.unl <- all.files[ grep( 'cohortlinked/us/unl' , all.files ) ]

				# extract the filepaths of the territories cohort-linked unlinked files
				cohortlinked.ps.unl <- all.files[ grep( 'cohortlinked/ps/unl' , all.files ) ]

				# extract the filepaths of the nationwide cohort-linked numerator files
				cohortlinked.us.num <- all.files[ grep( 'cohortlinked/us/num' , all.files ) ]

				# extract the filepaths of the territories cohort-linked numerator files
				cohortlinked.ps.num <- all.files[ grep( 'cohortlinked/ps/num' , all.files ) ]

				# extract the filepaths of the nationwide cohort-linked denominator files
				cohortlinked.us.den <- all.files[ grep( 'cohortlinked/us/den' , all.files ) ]

				# extract the filepaths of the territories cohort-linked denominator files
				cohortlinked.ps.den <- all.files[ grep( 'cohortlinked/ps/den' , all.files ) ]

				# throw out all non-digits to extract the year from the data file
				years <- gsub( "\\D" , "" , basename( cohortlinked.us.num ) )

				# for years after 2004, simply use the 2004 sas import scripts
				years[ years > 2004 ] <- 2004

				# if the year is 2004, don't add "/data" to the folder filepath.
				if ( years == 2004 ) y_d <- years else y_d <- paste0( years , "/data" )

				# build the character string containing the filepath
				# of the sas file of the cohort-linked numerator file
				num_ri <-
					paste0( 
						"http://www.nber.org/lbid/" , 
						y_d ,
						"/linkco" ,
						years ,
						"us_num.sas"
					)
				
				# build the character string containing the filepath
				# of the sas file of the cohort-linked denominator file
				den_ri <-
					paste0( 
						"http://www.nber.org/lbid/" , 
						y_d ,
						"/linkco" ,
						years ,
						"us_den.sas"
					)

				# build the character string containing the filepath
				# of the sas file of the cohort-linked unlinked file
				unl_ri <-
					paste0( 
						"http://www.nber.org/lbid/" , 
						y_d ,
						"/linkco" ,
						years ,
						"us_unl.sas"
					)

				# prepare the downloaded data-file and the sas importation script
				# for a read_SAScii_monetdb() call.  then - hello operator - make the call!
				nchs_import( 
					cohortlinked.us.num , 
					sas.scripts = num_ri ,
					db = db
				)

				# prepare the downloaded data-file and the sas importation script
				# for a read_SAScii_monetdb() call.  then - hello operator - make the call!
				nchs_import( 
					cohortlinked.us.den , 
					sas.scripts = den_ri ,
					db = db
				)

				# prepare the downloaded data-file and the sas importation script
				# for a read_SAScii_monetdb() call.  then - hello operator - make the call!
				nchs_import( 
					cohortlinked.us.unl , 
					sas.scripts = unl_ri ,
					db = db
				)

				# prepare the downloaded data-file and the sas importation script
				# for a read_SAScii_monetdb() call.  then - hello operator - make the call!
				nchs_import( 
					cohortlinked.ps.num , 
					sas.scripts = num_ri ,
					db = db
				)

				# prepare the downloaded data-file and the sas importation script
				# for a read_SAScii_monetdb() call.  then - hello operator - make the call!
				nchs_import( 
					cohortlinked.ps.den , 
					sas.scripts = den_ri ,
					db = db
				)

				# prepare the downloaded data-file and the sas importation script
				# for a read_SAScii_monetdb() call.  then - hello operator - make the call!
				nchs_import( 
					cohortlinked.ps.unl , 
					sas.scripts = unl_ri ,
					db = db ,
					azr = ( catalog[ i , 'year' ] == 2005 )
				)
				
				# delete all files in the "/cohortlinked/us" directory (the fifty states plus DC)
				file.remove( list.files( paste0( catalog[ i , 'output_folder' ] , '/cohortlinked/us/' ) , recursive = TRUE , full.names = TRUE ) )
				
				# delete all files in the "/cohortlinked/ps" directory (the fifty states plus DC)
				file.remove( list.files( paste0( catalog[ i , 'output_folder' ] , '/cohortlinked/ps/' ) , recursive = TRUE , full.names = TRUE ) )
				
			}


			# loop through every year specified by the user
			if( catalog[ i , 'type' ] == 'mortality' ){

				# for the current year, use a custom-built `nchs_extract_files` function
				# to determine the ftp location of the current mortality file you're workin' on.
				mortality <- nchs_extract_files( files[ grep( catalog[ i , 'year' ] , files ) ] , 'mortality' )
				
				# download the mortality file to the local working directory
				nchs_download( mortality , catalog[ i , 'output_folder' ] )

				# create a character vector containing all files in the current working directory
				all.files <- list.files( catalog[ i , 'output_folder' ] , recursive = TRUE , full.names = TRUE )

				# remove pdfs from possible identifier files
				all.files <- all.files[ !grepl( "\\.pdf$" , tolower( all.files ) ) ]
				
				# extract the filepaths of the nationwide mortality files
				mortality.us <- all.files[ grep( 'mortality/us/' , all.files ) ]

				# extract the filepaths of the territories mortality files
				mortality.ps <- all.files[ grep( 'mortality/ps/' , all.files ) ]

				# throw out all non-digits to extract the year from the data file
				year.plus.data <- years <- gsub( "\\D" , "" , basename( mortality.us ) )

				# the pre-2006 files have an extra "/data" in the filepath of the sas import script
				year.plus.data[ as.numeric( year.plus.data ) <= 2005 ] <- 
					paste( year.plus.data[ as.numeric( year.plus.data ) <= 2005 ] , 'data' , sep = '/' )
				

				# build the character string containing the filepath
				# of the sas file of the mortality file
				sas_ri <- paste0( "http://www.nber.org/mortality/" , year.plus.data , "/mort" , years , ".sas" )

				# throw out all non-digits to extract the year from the mortality territory file
				cap.at.1995 <- as.numeric( gsub( "\\D" , "" , basename( mortality.ps ) ) )

				# if the year is after named filepath..
				if ( catalog[ i , 'year' ] > cap.at.1995 ){
				
					# use the sas importation script as it is.
					terr_ri <- sas_ri
				
				# ..otherwise..
				} else {
				
					# use the `terr` filepath
					terr_ri <-
						paste0( 
							"http://www.nber.org/mortality/" , 
							cap.at.1995 ,
							"/terr" ,
							substr( cap.at.1995 , 3 , 4 ) ,
							".sas"
						)
					
				}
					
				# prepare the downloaded data-file and the sas importation script
				# for a read_SAScii_monetdb() call.  then - hello operator - make the call!
				nchs_import( 
					mortality.us , 
					sas.scripts = nchs_order_at_signs( sas_ri , add.blank = TRUE ) ,
					db = db
				)

				# prepare the downloaded data-file and the sas importation script
				# for a read_SAScii_monetdb() call.  then - hello operator - make the call!
				nchs_import( 
					mortality.ps , 
					sas.scripts = nchs_order_at_signs( sas_ri , add.blank = TRUE ) ,
					db = db
				)

				# delete all files in the "/mortality/us" directory (the fifty states plus DC)
				file.remove( list.files( paste0( catalog[ i , 'output_folder' ] , '/mortality/us/' ) , recursive = TRUE , full.names = TRUE ) )
				
				# delete all files in the "/mortality/ps" directory (the fifty states plus DC)
				file.remove( list.files( paste0( catalog[ i , 'output_folder' ] , '/mortality/ps/' ) , recursive = TRUE , full.names = TRUE ) )
				
			}


			# loop through every year specified by the user
			if( catalog[ i , 'type' ] == 'fetaldeath' ){

				# for 2007 and beyond, use the 2007 sas script.
				# otherwise use the 2006 sas script
				sas_ri <-
					ifelse(
						catalog[ i , 'year' ] >= 2007 ,
						system.file("extdata", "nvss/nchs_fetal_death_2007.sas", package = "lodown") ,
						system.file("extdata", "nvss/nchs_fetal_death_2006.sas", package = "lodown")
					)
					
				# build the full filepath of the fetal death zipped file
				fn <- 
					paste0( 
						"ftp://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/fetaldeathus/Fetal" , 
						catalog[ i , 'year' ] , 
						"US.zip" 
					)
					
				# read the fetal death nationwide zipped file directly into RAM with the sas importation script
				us <- read_SAScii( fn , sas_ri , zipped = TRUE )
				
				# convert all column names to lowercase
				names( us ) <- tolower( names( us ) )
				
				
				# build the full filepath of the fetal death zipped file
				fn <- 
					paste0( 
						"ftp://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/DVS/fetaldeathter/Fetal" , 
						catalog[ i , 'year' ] , 
						"PS.zip" 
					)
					
				# read the fetal death territory zipped file directly into RAM with the sas importation script
				ps <- read_SAScii( fn , sas_ri , zipped = TRUE )

				# convert all column names to lowercase
				names( ps ) <- tolower( names( ps ) )

				# save both data.frame objects to an R data file
				saveRDS( us , file = paste0( catalog[ i , 'output_folder' ] , "/fetal death " , catalog[ i , 'year' ] , " us.rds" ) , compress = FALSE )
				saveRDS( ps , file = paste0( catalog[ i , 'output_folder' ] , "/fetal death " , catalog[ i , 'year' ] , " ps.rds" ) , compress = FALSE )

				catalog[ i , 'case_count' ] <- nrow( us ) + nrow( ps )
				
			} else {
			
				tables_after <- setdiff( tables_before , DBI::dbListTables( db ) )
				
				for( this_table in tables_after ) catalog[ i , 'case_count' ] <- max( catalog[ i , 'case_count' ] , DBI::dbGetQuery( db , paste0( "SELECT COUNT(*) FROM " , this_table ) )[ 1 , 1 ] , na.rm = TRUE )
				
			}

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'output_folder' ] , "'\r\n\n" ) )

		}

		on.exit()
		
		catalog

	}


	
	

# this function reads through every line in a .dat file
# and converts unknown character types to ASCII,
# so monetdb will not break during data importation
clear.goofy.characters <-
	function( fn , fl ){
	
		tf <- tempfile()
		
		# initiate a read-only connection to the input file
		incon <- file( fn , "r")

		outcon <- file( tf , "w" )

		# loop through every row of data in the original input file
		while( length( line <- readLines( incon , 1 ) ) > 0 ){

			# remove goofy special characters (that will break monetdb)
			line <- iconv( line , "" , "ASCII" , " " )
		
			# if there's an enforced line length..
			if( fl ){
				# ..then confirm the current line matches that length before writing
				if( nchar( line ) == fl ) writeLines( line , outcon )
				
			} else {
				# otherwise just write it.
				writeLines( line , outcon )
			}
		}
		
		# close all file connections
		close( outcon )
		close( incon )
		
		tf
	}


# this function prepares and then executes a read_SAScii_monetdb call
nchs_import <-
	function(
		files.to.import ,
		sas.scripts ,
		db ,
		force.length = FALSE ,
		azr = FALSE
	){

		# figure out tablename from the files.to.import
		tablenames <- gsub( "(.*)/(.*)/(.*)/(.*)\\.dat" , "\\2_\\3_\\4" , files.to.import , ignore.case = TRUE )
	
		for ( i in seq( length( tablenames ) ) ){
	
			fti <- clear.goofy.characters( files.to.import[ i ] , fl = force.length )
			
			on.exit( suppressWarnings( while( any( unlink( fti ) ) ) Sys.sleep( 1 ) ) )
			
			read_SAScii_monetdb( 
				fn = fti ,
				sas_ri = nchs_remove_overlap( sas.scripts[ i ] ) , 
				beginline = 1 , 
				zipped = FALSE , 
				tl = TRUE ,						# convert all column names to lowercase?
				tablename = tablenames[ i ] ,
				overwrite = FALSE ,				# overwrite existing table?
				connection = db ,
				allow_zero_records = azr
			)
			
			suppressWarnings( while( unlink( fti ) ) Sys.sleep( 1 ) )
			
		}
		
		gc()
		
		TRUE
	}


# this function figures out the filepaths of all zipped and pdf files
# from the cdc's website that store mortality, cohort-linked, period-linked, natality, and fetal death files
nchs_extract_files <-
	function( y , name ){
	
		y <- y[ grep( name , y ) ]
	
		y <- tolower( y )

		pdfs <- y[ grep( "(ftp://ftp.cdc.gov/.*\\.pdf)" , y ) ]

		pdf.files <- gsub( "(.*a href=\\\")(ftp://ftp.cdc.gov/.*\\.pdf)(.*)$" , "\\2" , pdfs )

		if( length( pdf.files ) == 0 ) pdf.files <- NA
		
		zips <- y[ grep( "(ftp://ftp.cdc.gov/.*\\.zip)" , y ) ]

		ps <- zips[ grep( 'ps.zip' , zips ) ]
		us <- zips[ !grepl( 'ps.zip' , zips ) ]

		ps.files <- gsub( "(.*a href=\\\")(ftp://ftp.cdc.gov/.*\\.zip)(.*)$" , "\\2" , ps )
		us.files <- gsub( "(.*a href=\\\")(ftp://ftp.cdc.gov/.*\\.zip)(.*)$" , "\\2" , us )

		list( name = name , pdfs = pdf.files , ps = ps.files , us = us.files )
	}
	

# this function adds starting blanks to sas importation scripts
# that are not already available
nchs_add_blanks <-
	function( sasfile ){
		sas_lines <- tolower( readLines( sasfile ) )

		if( any( grepl( "@19   rectype        1." , sas_lines ) ) ){
		
			sas_lines <- gsub( "@19   rectype        1." , "@1 blank $ 18 @19   rectype        1." , sas_lines )
		
		} else if ( any( grepl( "@20   restatus       1." , sas_lines ) ) ) {
		
			sas_lines <- gsub( "@20   restatus       1." , "@1 blank $ 19 @20   restatus       1." , sas_lines )
		
		}
		
		
		# the column name `year` is illegal.
		sas_lines <- gsub( " year " , " yearz " , sas_lines )
		
		
		# create a temporary file
		tf <- tempfile()

		# write the updated sas input file to the temporary file
		writeLines( sas_lines , tf )

		# return the filepath to the temporary file containing the updated sas input script
		tf
	}


# this function extend missing blank field positions after frace
nchs_extend_frace <-
	function( sasfile ){
		sas_lines <- tolower( readLines( sasfile ) )

		
		sas_lines <- gsub( "@1443 frace8e       $3. " , "@1443 frace8e       $3. @1446 endblank $ 55." , sas_lines , fixed = TRUE )
				
		# create a temporary file
		tf <- tempfile()

		# write the updated sas input file to the temporary file
		writeLines( sas_lines , tf )

		# return the filepath to the temporary file containing the updated sas input script
		tf
	}

	
# this function re-orders lines in sas importion
# scripts, based on the @ sign positions
nchs_order_at_signs <-
	function( sasfile , add.blank = FALSE ){
		sas_lines <- tolower( readLines( sasfile ) )

		ats <- sas_lines[ substr( sas_lines , 1 , 1 ) == "@" ]

		positions <- as.numeric( substr( ats , 2 , 5 ) )

		sas_lines <- ats[ order( positions ) ]

		# if the first position is missing..
		if ( ( sort( positions )[ 1 ] != 1 ) & add.blank ){
		
			# ..add a blank column
			new.line <- paste( "@1 blank" , sort( positions )[ 1 ] - 1 )
			
			sas_lines <- c( new.line , sas_lines )
		}
				
		sas_lines <- c( "INPUT" , sas_lines , ";" )
		
		# create a temporary file
		tf <- tempfile()

		# write the updated sas input file to the temporary file
		writeLines( sas_lines , tf )

		# return the filepath to the temporary file containing the updated sas input script
		tf
	}

	
# this function to removes hard-coded overlapping columns
nchs_remove_overlap <-
	function( sasfile ){
		sas_lines <- tolower( readLines( sasfile ) )

		sas_lines <- sas_lines[ sas_lines != "@119  fipssto           $2. " ]
		sas_lines <- sas_lines[ sas_lines != "@124  fipsstr           $2. " ]
		
		sas_lines <- gsub( "@214 ucr130 3." , "@214 ucr130 $ 3." , sas_lines )
		
		sas_lines <- gsub( "@107  mrace6             2" , "@107  mrace6             1" , sas_lines )
		sas_lines <- gsub( "@9    dob_yy             4" , "@1 BLANK $8  @9    dob_yy             4" , sas_lines )
		sas_lines <- gsub( "@7    revision" , "@1    BLANK $6 @7    revision" , sas_lines )
		sas_lines <- gsub( "@4    reparea        1." , "@4    reparea        $1" , sas_lines )
		
		
		if ( sas_lines[ 25 ] == "                    @19 cntyocb 3." & sas_lines[ 26 ] == "                    @17 stateocb 2." ){
			sas_lines[ 25 ] <- "                    @17 stateocb 2."
			sas_lines[ 26 ] <- "                    @19 cntyocb 3."
		}
		
		overlap.fields <- 
			c( 'rdscresb' , 'regnresb' , 'divresb' , 'estatrsb' , 'cntyresb' , 'statersb' , 'rdsscocd' , 'regnoccd' , "regnocc" , "regnres" ,
				'divoccd' , 'estatocd' , 'cntyocd' , 'stateocd' , 'rdscresd' , 'regnresd' , 'divresd' , 'estatrsd' , 
				'cntyresd' , 'statersd' , 'cityresd' , 'cityresb' , 'rdsscocb' , 'regnoccb' , 'divoccb' , 'estatocb' , 'cntyocb' , 
				'stateocb' , 'stateoc' , 'staters' , paste0( "rnifla_" , 1:14 ) , paste0( 'entity' , 1:14 ) , 'divocc' , 
				'statenat' , 'stoccfip' , 'divres' , 'stateres' , 'stresfip' , 'feduc6' ,
				'stocfipb' , 'strefipb' , 'delmeth' , 'medrisk' , 'otherrsk' , 'obstetrc' , 'labor' , 'newborn' , 'congenit' , 'flres' , 
				paste0( 'rnifla_' , 1:9 ) , paste0( 'rnifl_' , 10:20 ) , paste0( 'entity_' , 1:9 ) , paste0( 'entit_' , 10:20 ) , 
				paste0( 'enifla_' , 1:9 ) , paste0( 'enifl_' , 10:20 ) , 'stocfipd' , 'strefipd'
			)
		
		sas_lines <- sas_lines[ !grepl( paste( overlap.fields , collapse = "|" ) , sas_lines ) ]

		
		# the column name `year` is illegal.
		sas_lines <- gsub( " year " , " yearz " , sas_lines )
		
		
		# create a temporary file
		tf <- tempfile()

		# write the updated sas input file to the temporary file
		writeLines( sas_lines , tf )

		# return the filepath to the temporary file containing the updated sas input script
		tf
	}
	

# this function downloads a specified zipped file to the local disk
# and unzips everything according to a straightforward pattern
nchs_download <-
	function( y , output_folder ){
		
		tf <- tempfile()
		
		winrar.dir <- normalizePath( paste( output_folder , "winrar" , sep = "/" ) )
		
		on.exit( unlink( winrar.dir , recursive = TRUE ) )
		
		on.exit( unlink( tf ) )
		
		dir.create( paste0( output_folder , "/" , y$name ) )
		
		dir.create( paste( output_folder , y$name , "us" , sep = "/" ) )
		
		dir.create( paste( output_folder , y$name , "ps" , sep = "/" ) )
		
		for ( i in c( y$us , y$ps ) ){
			
			curYear <- as.numeric( gsub( "\\D" , "" , i ) )
			if ( curYear < 50 ) curYear <- curYear + 2000
			if ( curYear > 50 & curYear < 100 ) curYear <- curYear + 1900
			
			cachaca( i , tf , mode = 'wb' )
			
			# actually run winrar on the downloaded file,
			# extracting the results to the temporary directory
			
			# extract the file, platform-specific
			
			archive::archive_extract( tf , dir = winrar.dir )

			suppressWarnings( while( any( file.remove( tf ) ) ) Sys.sleep( 1 ) )
			
			z <- list.files( winrar.dir , full.names = TRUE )
			
			if ( y$name %in% c( 'mortality' , 'natality' , 'fetaldeath' ) ){
				
				if ( y$name == 'mortality' & curYear == 1994 ){
				
					# add the puerto rico file to the guam file
					file.append( z[ 1 ] , z[ 2 ] )
					# add the virgin islands file to the puerto rico + guam file
					file.append( z[ 1 ] , z[ 3 ] )
					
					# remove those two files from the disk
					suppressWarnings( while( any( file.remove( z[ 2 ] , z[ 3 ] ) ) ) Sys.sleep( 1 ) )
					
					# remove those two files from the vector
					z <- z[ -2:-3 ]
				
				}
				
				file.copy( 
					z , 
					paste( 
						output_folder , 
						y$name , 
						ifelse( i %in% y$us , "us" , "ps" ) , 
						paste0( "x" , curYear , ".dat" ) , 
						sep = "/" 
					) 
				)
				
			} else {
			
				# confirm it's got at least two files..
				stopifnot( length( z ) > 1 )
				# some years don't have unlinked, so this test is not necessary
				# stopifnot( any( un <- grepl( 'un' , z ) ) )
				
				stopifnot( any( num <- grepl( 'num' , tolower( z ) ) ) )
				
				stopifnot( any( den <- grepl( 'den' , tolower( z ) ) ) )
			
				if ( any( un <- grepl( 'un' , tolower( z ) ) ) ){
				
					file.copy( z[ un ] , paste( output_folder , y$name , ifelse( i %in% y$us , "us" , "ps" ) , paste0( "unl" , curYear , ".dat" ) , sep = "/" ) )
				
				}
				
				file.copy( z[ num ] , paste( output_folder , y$name , ifelse( i %in% y$us , "us" , "ps" ) , paste0( "num" , curYear , ".dat" ) , sep = "/" ) )
				
				file.copy( z[ den ] , paste( output_folder , y$name , ifelse( i %in% y$us , "us" , "ps" ) , paste0( "den" , curYear , ".dat" ) , sep = "/" ) )
				
			}
			
			suppressWarnings( while( unlink( z ) ) Sys.sleep( 1 ) )
			
		}	
		
		
		for ( i in y$pdfs[ !is.na( y$pdfs ) ] ){
		
				
			attempt.one <- try( cachaca( i , paste( output_folder , y$name , basename( i ) , sep = "/" ) , mode = 'wb' ) , silent = TRUE )
			
			if ( class( attempt.one ) == 'try-error' ) {
				Sys.sleep( 60 )
				
				cachaca( i , paste( output_folder , y$name , basename( i ) , sep = "/" ) , mode = 'wb' )
			}
		}
			
		TRUE
	}

nvss_max_year <-
	function( folder , files ){
	
		available_years <- gsub( "[^0-9]" , "" , basename( gsub( '(.*)zip\">(.*) \\((.*)' , "\\2" , grep( folder , files , value = TRUE ) ) ) )
		
		max( as.numeric( available_years[ nchar( available_years ) == 4 ] ) )
	}
		