get_catalog_uspums <-
	function( data_name = "uspums" , output_dir , ... ){

		states.plus.dc <-
			gsub(
				" " ,
				"_" ,
				c( "District of Columbia" , datasets::state.name )
			)

		# same deal with state abbreviations..  and add in fips code with left-side zeroes for the one-digit ones.
		st <-
			data.frame(

				state.abb = c( "DC" , datasets::state.abb ) ,
			
				state.name = states.plus.dc ,
				
				state.fips = 
					stringr::str_pad( 
						c( 11 , 1 , 2 , 4:6 , 8:10 , 12:13 , 15:42 , 44:51 , 53:56 ) , 
						width = 2 , 
						pad = "0"
					)
			)

		st <- rbind( st , data.frame( state.abb = "PR" , state.name = "Puerto_Rico" , state.fips = "72" ) )
		
		catalog <- merge( data.frame( year = c( 1990 , 1990 , 2000 , 2000 , 2010 ) , percent = c( 1 , 5 , 1 , 5 , 10 ) ) , st )
		
		catalog <- catalog[ !( catalog$year == 1990 & catalog$state.abb == "PR" ) , ]
		
		catalog[ catalog$year == 2010 & catalog$percent == 10 , 'full_url' ] <-
			paste0( "https://www2.census.gov/census_2010/12-Stateside_PUMS/" , catalog[ catalog$year == 2010 & catalog$percent == 10 , 'state.name' ] , "/" , tolower( catalog[ catalog$year == 2010 & catalog$percent == 10 , 'state.abb' ] ) , ".2010.pums.01.txt" )
		
		catalog[ catalog$year == 2000 & catalog$percent == 1 , 'full_url' ] <-
			paste0( "https://www2.census.gov/census_2000/datasets/PUMS/OnePercent/" , catalog[ catalog$year == 2000 & catalog$percent == 1 , 'state.name' ] , "/revisedpums1_" , catalog[ catalog$year == 2000 & catalog$percent == 1 , 'state.fips' ] , ".txt" )
		
		catalog[ catalog$year == 2000 & catalog$percent == 5 , 'full_url' ] <- 
			paste0( "https://www2.census.gov/census_2000/datasets/PUMS/FivePercent/" , catalog[ catalog$year == 2000 & catalog$percent == 5 , 'state.name' ] , "/REVISEDPUMS5_" , catalog[ catalog$year == 2000 & catalog$percent == 5 , 'state.fips' ] , ".TXT" )
		
		catalog[ catalog$year == 1990 & catalog$percent == 1 , 'full_url' ] <-
			paste0( "https://www2.census.gov/census_1990/pums_1990_b/PUMSBX" , catalog[ catalog$year == 1990 & catalog$percent == 1 , 'state.abb' ] , ".zip" )
		
		catalog[ catalog$year == 1990 & catalog$percent == 5 , 'full_url' ] <-
			paste0( "https://www2.census.gov/census_1990/1990_PUMS_A/PUMSAX" , catalog[ catalog$year == 1990 & catalog$percent == 5 , 'state.abb' ] , ".zip" )
		
		
		catalog$dbfile <- paste0( output_dir , "/SQLite.db" )
		
		catalog$design <- paste0( output_dir , '/pums_' , catalog$year , '_' , catalog$percent , '_m.rds' )
		
		catalog$merged_tablename <- paste0( 'pums_' , catalog$year , '_' , catalog$percent , '_m' )
		catalog$household_tablename <- paste0( 'pums_' , catalog$year , '_' , catalog$percent , '_h' )
		catalog$person_tablename <- paste0( 'pums_' , catalog$year , '_' , catalog$percent , '_p' )
		
		catalog$hh_structure <- paste0( "hh." , substr( catalog$year , 3 , 4 ) , ".structure" )
		catalog$person_structure <- paste0( "person." , substr( catalog$year , 3 , 4 ) , ".structure" )
		
		catalog

	}


lodown_uspums <-
	function( data_name = "uspums" , catalog , ... ){

		on.exit( print( catalog ) )

		if ( !requireNamespace( "readxl" , quietly = TRUE ) ) stop( "readxl needed for this function to work. to install it, type `install.packages( 'readxl' )`" , call. = FALSE )
		
		if ( !requireNamespace( "descr" , quietly = TRUE ) ) stop( "descr needed for this function to work. to install it, type `install.packages( 'descr' )`" , call. = FALSE )

		# # # # # # # # # #
		# structure files #
		# # # # # # # # # #


		# # # # # # 1990 # # # # # #

		# if 1990 was requested in either the 1% or 5% files..
		if ( 1990 %in% catalog$year ){

			# create a temporary file on the local disk
			tf <- tempfile()
			of <- tempfile()
			
			# download the pums sas script provided by the census bureau
			cachaca( "https://www2.census.gov/census_1990/1990_PUMS_A/TOOLS/sas/PUMS.SAS" , tf , mode = 'wb' )
			
			# read the script into working memory
			sas.90 <- readLines( tf )

			# add a leading column (parse.SAScii cannot handle a sas importation script that doesn't start at the first position)
			sas.90 <- gsub( "@2 SerialNo $ 7." , "@1 rectype $ 1 @2 SerialNo $ 7." , sas.90 , fixed = TRUE )

			# write the script back to memory
			writeLines( sas.90 , of )

			# read in the household structure
			suppressWarnings( hh.90.structure <- SAScii::parse.SAScii( of , beginline = 7 ) )
			
			# read in the person structure
			suppressWarnings( person.90.structure <- SAScii::parse.SAScii( of , beginline = 125 ) )
			
			# convert both variables to lowercase
			hh.90.structure$variable <- tolower( hh.90.structure$varname )
			person.90.structure$variable <- tolower( person.90.structure$varname )

			# find the starting and ending positions of all rows, in both tables (needed for monet_read_tsv later)
			hh.90.structure$beg <- cumsum( abs( hh.90.structure$width ) ) - abs( hh.90.structure$width ) + 1
			hh.90.structure$end <- cumsum( abs( hh.90.structure$width ) )

			person.90.structure$beg <- cumsum( abs( person.90.structure$width ) ) - abs( person.90.structure$width ) + 1
			person.90.structure$end <- cumsum( abs( person.90.structure$width ) )

			# rename all empty columns `blank_#` in both tables
			if ( any( blanks <- is.na( hh.90.structure$variable ) ) ){
				hh.90.structure[ is.na( hh.90.structure$variable ) , 'variable' ] <- paste0( "blank_" , 1:sum( blanks ) )
			}

			if ( any( blanks <- is.na( person.90.structure$variable ) ) ){
				person.90.structure[ is.na( person.90.structure$variable ) , 'variable' ] <- paste0( "blank_" , 1:sum( blanks ) )
			}

			# `sample` is an illegal column name in monetdb, so change it in both tables
			hh.90.structure[ hh.90.structure$variable == 'sample' , 'variable' ] <- 'sample_'
			person.90.structure[ person.90.structure$variable == 'sample' , 'variable' ] <- 'sample_'

			hh.90.structure[ hh.90.structure$variable == 'value' , 'variable' ] <- 'value_'
			person.90.structure[ person.90.structure$variable == 'value' , 'variable' ] <- 'value_'

		}


		# # # # # # 2000 # # # # # #

		# if 2000 was requested in either the 1% or 5% files..
		if ( 2000 %in% catalog$year ){

			# create a temporary file on the local disk
			pums.layout <- paste0( tempfile() , '.xls' )

			# download the layout excel file
			cachaca( "https://www2.census.gov/census_2000/datasets/PUMS/FivePercent/5%25_PUMS_record_layout.xls" , pums.layout , mode = 'wb' )

			# initiate a quick layout read-in function #
			code.str <-
				function( fn , sheet ){

					# read the sheet (specified as a function input) to an object `stru
					stru <- data.frame( readxl::read_excel( fn , sheet = sheet , skip = 1 ) )
					
					# make all column names of the `stru` data.frame lowercase
					names( stru ) <- tolower( names( stru ) )
					
					# remove leading and trailing whitespace, and convert everything to lowercase
					# in the `variable` column of the `stru` table
					stru$variable <- stringr::str_trim( tolower( stru$variable ) )
					
					# coerce these two columns to numeric
					stru[ c( 'beg' , 'end' ) ] <- sapply( stru[ c( 'beg' , 'end' ) ] , as.numeric )
					
					# keep only four columns, and only unique records from the `stru` table
					stru <- unique( stru[ , c( 'beg' , 'end' , 'a.n' , 'variable' ) ] )
					
					# throw out records missing a beginning position
					stru <- stru[ !is.na( stru$beg ) , ]
					
					# calculate the width of each field
					stru$width <- stru$end - stru$beg + 1
					
					# remove overlapping fields
					stru <- 
						stru[ 
							!( stru$variable %in% 
								c( 'ancfrst1' , 'ancscnd1' , 'lang1' , 'pob1' , 'migst1' , 'powst1' , 'occcen1' , 'occsoc1' , 'filler' ) ) , ]
					
					# remove fields that are invalid in monetdb
					stru[ stru$variable == "sample" , 'variable' ] <- 'sample_'
					stru[ stru$variable == "value" , 'variable' ] <- 'value_'
			
					hardcoded.numeric.columns <-
						c( "serialno" , "hweight" , "persons" , "elec" , "gas" , "water" , "oil" , "rent" , "mrt1amt" , "mrt2amt" , "taxamt" , "insamt" , "condfee" , "mhcost" , "smoc" , "smocapi" , "grent" , "grapi" , "hinc" , "finc" , "pweight" , "age" , "ancfrst5" , "ancscnd5" , "yr2us" , "trvtime" , "weeks" , "hours" , "incws" , "incse" , "incint" , "incss" , "incssi" , "incpa" , "incret" , "incoth" , "inctot" , "earns" , "poverty" )
			
					# add a logical `char` field to both of these data.frames
					stru$char <- ( stru$a.n %in% 'A' & !( stru$variable %in% hardcoded.numeric.columns ) )
								
					# since this is the last line of the function `code.str`
					# whatever this object `stru` is at the end of the function
					# will be _returned_ by the function
					stru
				}

			# read in the household file structure from excel sheet 1
			hh.00.structure <- code.str( pums.layout , 1 )

			# read in the person file structure from excel sheet 2
			person.00.structure <- code.str( pums.layout , 2 )
			
		}


		# # # # # # 2010 # # # # # #

		# if 2010 was requested in the 10% files..
		if ( 2010 %in% catalog$year ){

			# create a temporary file on the local disk
			pums.layout <- paste0( tempfile() , ".xlsx" )

			# download the layout excel file
			cachaca( "https://www2.census.gov/census_2010/12-Stateside_PUMS/2010%20PUMS%20Record%20Layout.xlsx" , pums.layout , mode = 'wb' )

			# initiate a quick layout read-in function #
			code.str <-
				function( fn , sheet ){

					# read the sheet (specified as a function input) to an object `stru
					stru <- data.frame( readxl::read_excel( fn , sheet = sheet , skip = 1 ) )
					
					# make all column names of the `stru` data.frame lowercase
					names( stru ) <- tolower( names( stru ) )
					
					# remove leading and trailing whitespace, and convert everything to lowercase
					# in the `variable` column of the `stru` table
					stru$variable <- stringr::str_trim( tolower( stru$variable ) )
					
					# keep only four columns, and only unique records from the `stru` table
					stru <- unique( stru[ , c( 'beg' , 'end' , 'a.n' , 'variable' ) ] )
					
					# throw out records missing a beginning position
					stru <- stru[ !is.na( stru$beg ) , ]
					
					# calculate the width of each field
					stru$width <- stru$end - stru$beg + 1
					
					# remove racedet duplicate
					stru <- stru[ !is.na( stru$beg ) , ]
					
					# remove fields that are invalid in monetdb
					stru[ stru$variable == "sample" , 'variable' ] <- 'sample_'
			
					hardcoded.numeric.columns <-
						c( "serialno" , "hweight" , "persons" , "elec" , "gas" , "water" , "oil" , "rent" , "mrt1amt" , "mrt2amt" , "taxamt" , "insamt" , "condfee" , "mhcost" , "smoc" , "smocapi" , "grent" , "grapi" , "hinc" , "finc" , "pweight" , "age" , "ancfrst5" , "ancscnd5" , "yr2us" , "trvtime" , "weeks" , "hours" , "incws" , "incse" , "incint" , "incss" , "incssi" , "incpa" , "incret" , "incoth" , "inctot" , "earns" , "poverty" )
			
					# add a logical `char` field to both of these data.frames
					stru$char <- ( stru$a.n %in% 'A' & !( stru$variable %in% hardcoded.numeric.columns ) )
								
					# since this is the last line of the function `code.str`
					# whatever this object `stru` is at the end of the function
					# will be _returned_ by the function
					stru
				}

			# read in the household file structure from excel sheet 1
			hh.10.structure <- code.str( pums.layout , 1 )

			# read in the person file structure from excel sheet 2
			person.10.structure <- code.str( pums.layout , 2 )
			
		}

		
		unique_designs <- unique( catalog[ , c( 'year' , 'design' , 'hh_structure' , 'person_structure' , 'merged_tablename' , 'household_tablename' , 'person_tablename' , 'dbfile' ) ] )
		
		for( i in seq_len( nrow( unique_designs ) ) ){

			# open the connection to the monetdblite database
			db <- DBI::dbConnect( RSQLite::SQLite() , unique_designs[ i , 'dbfile' ] )

		
			these_files <- merge( catalog , unique_designs[ i , ] )[ , 'full_url' ]
		
				
				
			# run the `get.tsv` function on each of the files specified in the character vector (created above)
			# and provide a corresponding file number parameter for each character string.
			these_tsvs <-
				mapply(
					get.tsv ,
					these_files ,
					fileno = seq( length( these_files ) ) ,
					MoreArgs = 
						list(
							zipped = unique_designs[ i , 'year' ] == 1990 ,
							hh.stru = get( unique_designs[ i , "hh_structure" ] ) ,
							person.stru = get( unique_designs[ i , "person_structure" ] )
						)
				)
				

			# using the monetdb connection, import each of the household- and person-level tab-separated value files
			# into the database, naming the household, person, and also merged file with these character strings
			this_design <-
				pums.import.merge.design(
					db = db , 
					fn = these_tsvs, 
					merged.tn = unique_designs[ i , "merged_tablename" ] , 
					hh.tn = unique_designs[ i , "household_tablename" ] , 
					person.tn = unique_designs[ i , "person_tablename" ] ,
					hh.stru = get( unique_designs[ i , "hh_structure" ] ) ,
					person.stru = get( unique_designs[ i , "person_structure" ] )
				)

			# save the monetdb-backed complex sample survey design object to the local disk		
			saveRDS( this_design , file = unique_designs[ i , 'design' ] , compress = FALSE )
			
			catalog[ catalog$design == unique_designs[ i , 'design' ] , 'case_count' ] <- nrow( this_design )
			
			cat( paste0( data_name , " survey design entry " , i , " of " , nrow( unique_designs ) , " stored at '" , unique_designs[ i , 'design' ] , "'\r\n\n" ) )
			
		}
		
		on.exit()
		
		catalog

	}

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	

	
# construct a function that..
# takes an ascii file to be downloaded and two household/structure files,
# parses it, and saves it to a temporary file on the local disk as a tab-separated value file
get.tsv <-
	function(
		fp ,
		zipped ,
		hh.stru ,
		person.stru ,
		fileno
	){
		
		# store the warnings into a variable
		previous.warning.setting <- getOption( "warn" )
		
		# store encoding into a variable
		previous.encoding <- getOption( "encoding" )

		# at the end of this function, put the warning option
		# back to its original setting
		on.exit( options( "warn" = previous.warning.setting ) )
		on.exit( options( "encoding" = previous.encoding ) )

		# set warnings to behave like errors, so if a download
		# does not complete properly, the program re-tries.
		options( "warn" = 2 )
		
		# specify a temporary file on the local disk
		dlfile <- tempfile()
		txt_file <- tempfile()
		
		cachaca( fp , dlfile , mode = 'wb' )
		
		# the warning breakage can end now..
		options( "warn" = previous.warning.setting )
		# ..since the file has definitely downloaded properly.

		# if the downloaded file was a zipped file,
		# unzip it and replace it with its decompressed contents
		if ( zipped ) {
		
			tf_zip <- tempfile()
		
			tf_zip <- unzip_warn_fail( dlfile , exdir = tempdir() )
			
			txt_file <- tf_zip
		
		} else {
		
			file.copy( dlfile , txt_file )
		
		}
		
		# create two more temporary files
		tf.household <- tempfile()
		tf.person <- tempfile()
		
		# initiate a read-only connection to the input file
		incon <- file( txt_file , "rb")

		# initiate two write-only file connections "w" - pointing to the household and person files
		outcon.household <- file( tf.household , "w" )
		outcon.person <- file( tf.person , "w" )

		# start line counter #
		line.num <- 0

		# loop through every row of data in the original input file
		while( length( line <- readLines( incon , 1 , skipNul = TRUE ) ) > 0 ){

			if ( line.num > 1 ){
				# remove goofy special characters (that will break monetdb)
				thisline.to.ascii <- try( line <- gsub( "z" , " " , line , fixed = TRUE ) , silent = TRUE )
				
				if ( class( thisline.to.ascii ) == 'try-error' ){
					line <- iconv( line , "" , "ASCII" , sub = " " )		
					line <- gsub( "z" , " " , line , fixed = TRUE )
				}
				
				line <- gsub( "m99" , " 99" , line , fixed = TRUE )
				line <- gsub( "j" , " " , line , fixed = TRUE )
			}
			
			line <- gsub( "[^[:alnum:]///' ]" , " " ,  line )
			
			line <- iconv( line , "" , "ASCII" , sub = " " )

			line <- gsub( "P00083710210010540112000012110014100000028401800020193999910000000200000000000000000000000000000000000000p" , "P0008371021001054011200001211001410000002840180002019399991000000020000000000000000000000000000000000000000" , line , fixed = TRUE )
			line <- gsub( "H000837  623180140050999900 90012801000002005122050000000531112111521" , "H000837623180140050999900 90012801000002005122050000000531112111521" , line , fixed = TRUE )
			# end of goofy special character removal
			
			# ..and if the first character is a H, add it to the new household-only pums file.
			if ( substr( line , 1 , 1 ) == "H" ) {
				writeLines( 
					paste0(
						substr( 
							# add the line number at the end
							line ,
							1 , 
							cumsum( abs( hh.stru$width ) )[ nrow( hh.stru ) ]
						) , 
						stringr::str_pad( fileno , 10 ) 
					) , 
					outcon.household 
				)
			}
				
			# ..and if the first character is a P, add it to the new person-only pums file.
			if ( substr( line , 1 , 1 ) == "P" ) {
				writeLines( 
					paste0(
						substr( 
							# add the line number at the end
							line ,
							1 , 
							cumsum( abs( person.stru$width ) )[ nrow( person.stru ) ]
						) , 
						stringr::str_pad( fileno , 10 ) 
					)  , 
					outcon.person 
				)				
			}
				
			# add to the line counter #
			line.num <- line.num + 1

			# every 10k records, print current progress to the screen
			if ( line.num %% 10000 == 0 ) cat( " " , prettyNum( line.num , big.mark = "," ) , "census pums lines processed" , "\r" )

		}
		
		# close all file connections
		close( outcon.household )
		close( outcon.person )
		close( incon )
		
		# now we've got `tf.household` and `tf.person` on the local disk instead.
		# these have one record per household and one record per person, respectively.

		# create a place to store the tab-separated value file on the local disk
		hh.tsv <- tempfile()
		
		# convert..
		descr::fwf2csv( 
			# the household-level file
			tf.household , 
			# to a tsv file
			hh.tsv ,
			# with these column names
			names = c( hh.stru$variable , 'fileno' ) ,
			# starting positions
			begin = c( hh.stru$beg , hh.stru$end[ nrow( hh.stru ) ] + 1 ) ,
			# ending positions
			end = c( hh.stru$end , hh.stru$end[ nrow( hh.stru ) ] + 10 )
		)

		# remove the pre-tsv file
		file.remove( tf.household )


		# create a place to store the tab-separated value file on the local disk
		person.tsv <- tempfile()
		
		# convert..
		descr::fwf2csv( 
			# the person-level file
			tf.person , 
			# to a tsv file
			person.tsv , 
			# with these column names
			names = c( person.stru$variable , 'fileno' ) ,
			# starting positions
			begin = c( person.stru$beg , person.stru$end[ nrow( person.stru ) ] + 1 ) ,
			# ending positions
			end = c( person.stru$end , person.stru$end[ nrow( person.stru ) ] + 10 )
		)

		# remove the pre-tsv file
		file.remove( tf.person )
		
		options( "encoding" = previous.encoding )

		# return a character vector (of length two) containing the location on the local disk
		# where the household-level and person-level tsv files have been saved.
		c( hh.tsv , person.tsv )
	}


# construct a function that..
# takes a character vector full of tab-separated files stored on the local disk
# imports them all into monetdb, merges (rectangulates) them into a merged (_m) table,
# and finally creates a survey design object
pums.import.merge.design <-
	function( db , fn , merged.tn , hh.tn , person.tn , hh.stru , person.stru ){
		
		# extract the household tsv file locations
		hh.tfs <- as.character( fn[ 1 , ] )
		
		# extract the person tsv file locations
		person.tfs <- as.character( fn[ 2 , ] )

		# read one of the household-level files into RAM..
		hh.h <- read.table( hh.tfs[1], header = TRUE , sep = '\t' , na.strings = "NA" )
		
		# unique(sapply( hh.h , dbDataType , dbObj = db ))
		
		# count the number of records in each file
		hh.lines <- sapply( hh.tfs , R.utils::countLines )

		# read one of the person-level files into RAM..
		person.h <- read.table( person.tfs[1], header = TRUE , sep = '\t' , na.strings = "NA" )
		
		# unique(sapply( person.h , dbDataType , dbObj = db ))
		
		# count the number of records in each file
		person.lines <- sapply( person.tfs , R.utils::countLines )

		# use the monet_read_tsv function
		# to read the household files into a table called `hh.tn` in the monet database
		monet_read_tsv(
			db ,
			hh.tfs ,
			hh.tn ,
			nrows = hh.lines ,
			structure = hh.h ,
			nrow.check = 10000 ,
			lower.case.names = TRUE
		)

		# use the monet_read_tsv function
		# to read the household files into a table called `hh.tn` in the monet database
		monet_read_tsv(
			db ,
			person.tfs ,
			person.tn ,
			nrows = person.lines ,
			structure = person.h ,
			nrow.check = 10000 ,
			lower.case.names = TRUE
		)
		
		# remove blank_# fields in the monetdb household table
		lf <- DBI::dbListFields( db , hh.tn )
		hh.blanks <- lf[ grep( 'blank_' , lf ) ]
		for ( i in hh.blanks ) DBI::dbSendQuery( db , paste( 'alter table' , hh.tn , 'drop column' , i ) )

		# remove blank_# fields in the monetdb person table
		lf <- DBI::dbListFields( db , person.tn )
		person.blanks <- lf[ grep( 'blank_' , lf ) ]
		for ( i in person.blanks ) DBI::dbSendQuery( db , paste( 'alter table' , person.tn , 'drop column' , i ) )


		# intersect( DBI::dbListFields( db , hh.tn ) , DBI::dbListFields( db , person.tn ) )

		# find overlapping fields
		nonmatch.fields <- 
			paste0( 
				"b." , 
				DBI::dbListFields( db , person.tn )[ !( DBI::dbListFields( db , person.tn ) %in% DBI::dbListFields( db , hh.tn ) ) ] , 
				collapse = ", "
			)

		# create the merge statement
		ij <- 
			paste( 
				"create table" ,
				merged.tn , 
				"as select a.* ," , 
				nonmatch.fields , 
				"from" , 
				hh.tn ,
				"as a inner join" , 
				person.tn , 
				"as b on a.fileno = b.fileno AND a.serialno = b.serialno" 
			)
		
		# create a new merged table (named according to the input parameter `merged.tn`
		DBI::dbSendQuery( db , ij )

		# modify the `rectype` column for this new merged table so it's all Ms
		DBI::dbSendQuery( db , paste( "update" , merged.tn , "set rectype = 'M'" ) )

		# confirm that the number of records in the merged file
		# matches the number of records in the person file
		stopifnot( 
			DBI::dbGetQuery( db , paste( "select count(*) as count from" , merged.tn ) ) == 
			DBI::dbGetQuery( db , paste( "select count(*) as count from" , person.tn ) )
		)

		# add a column containing all ones to the current table
		DBI::dbSendQuery( db , paste0( 'alter table ' , merged.tn , ' add column one int' ) )
		DBI::dbSendQuery( db , paste0( 'UPDATE ' , merged.tn , ' SET one = 1' ) )
		
		
		# create a survey complex sample design object
		pums.design <-
			survey::svydesign(
				weight = if( grepl( "1990" , merged.tn ) ) ~pwgt1 else ~pweight ,			# weight variable column
				id = ~1 ,					# sampling unit column (defined in the character string above)
				data = merged.tn ,			# table name within the monet database (defined in the character string above)
				dbtype = "SQLite" ,
				dbname = DBI::dbGetInfo( db )$gdk_dbpath
			)
		# ..and return that at the end of the function.
		pums.design
	}
