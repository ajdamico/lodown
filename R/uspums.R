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
			paste0( "http://www2.census.gov/census_2010/12-Stateside_PUMS/" , catalog[ catalog$year == 2010 & catalog$percent == 10 , 'state.name' ] , "/" , tolower( catalog[ catalog$year == 2010 & catalog$percent == 10 , 'state.abb' ] ) , ".2010.pums.01.txt" )
		
		catalog[ catalog$year == 2000 & catalog$percent == 1 , 'full_url' ] <-
			paste0( "http://www2.census.gov/census_2000/datasets/PUMS/OnePercent/" , catalog[ catalog$year == 2000 & catalog$percent == 1 , 'state.name' ] , "/revisedpums1_" , catalog[ catalog$year == 2000 & catalog$percent == 1 , 'state.fips' ] , ".txt" )
		
		catalog[ catalog$year == 2000 & catalog$percent == 5 , 'full_url' ] <- 
			paste0( "http://www2.census.gov/census_2000/datasets/PUMS/FivePercent/" , catalog[ catalog$year == 2000 & catalog$percent == 5 , 'state.name' ] , "/REVISEDPUMS5_" , catalog[ catalog$year == 2000 & catalog$percent == 5 , 'state.fips' ] , ".TXT" )
		
		catalog[ catalog$year == 1990 & catalog$percent == 1 , 'full_url' ] <-
			paste0( "http://www2.census.gov/census_1990/pums_1990_b/PUMSBX" , catalog[ catalog$year == 1990 & catalog$percent == 1 , 'state.abb' ] , ".zip" )
		
		catalog[ catalog$year == 1990 & catalog$percent == 5 , 'full_url' ] <-
			paste0( "http://www2.census.gov/census_1990/1990_PUMS_A/PUMSAX" , catalog[ catalog$year == 1990 & catalog$percent == 5 , 'state.abb' ] , ".zip" )
		
		
		catalog$dbfolder <- paste0( output_dir , "/MonetDB" )
		
		catalog$design <- paste0( output_dir , '/pums_' , catalog$year , '_' , catalog$percent , '_m.rda' )
		
		catalog

	}


lodown_uspums <-
	function( data_name = "uspums" , catalog , ... ){

		if ( !requireNamespace( "gdata" , quietly = TRUE ) ) stop( "gdata needed for this function to work. to install it, type `install.packages( 'gdata' )`" , call. = FALSE )
		if ( !requireNamespace( "descr" , quietly = TRUE ) ) stop( "descr needed for this function to work. to install it, type `install.packages( 'descr' )`" , call. = FALSE )

		tf <- tempfile()


		for ( i in seq_len( nrow( catalog ) ) ){

			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

			unzipped_files <- unzip( tf , exdir = paste0( tempdir() , "/unzips" ) )






			# delete the temporary files
			suppressWarnings( file.remove( tf , unzipped_files ) )

		}

		invisible( TRUE )

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
		
		# construct two missing objects
		attempt1 <- attempt2 <- NA
		
		# specify a temporary file on the local disk
		dlfile <- tempfile()
		txt_file <- tempfile()
		
		# try to download the text file
		attempt1 <- try( cachaca( fp , dlfile , mode = 'wb' ) , silent = TRUE )
		
		# if the first attempt returned an error..
		if ( class( attempt1 ) == 'try-error' ) {
		
			# wait sixty seconds
			Sys.sleep( 60 )
			
			# and try again
			attempt2 <- try( cachaca( fp , dlfile , mode = 'wb' ) , silent = TRUE )
			
		}	
		
		# if the second attempt returned an error..
		if ( class( attempt2 ) == 'try-error' ) {
		
			# wait two minutes
			Sys.sleep( 120 )
			
			# and try one last time.
			cachaca( fp , dlfile , mode = 'wb' )
			# since there's no `try` function encapsulating this one,
			# it will break the whole program if it doesn't work
		}
		
		# the warning breakage can end now..
		options( "warn" = previous.warning.setting )
		# ..since the file has definitely downloaded properly.

		# if the downloaded file was a zipped file,
		# unzip it and replace it with its decompressed contents
		if ( zipped ) {
		
			tf_zip <- tempfile()
		
			tf_zip <- unzip( dlfile , exdir = tempdir() )
			
			txt_file <- tf_zip
		
		} else {
		
			file.copy( dlfile , txt_file )
		
		}
		
		# create two more temporary files
		tf.household <- tempfile()
		tf.person <- tempfile()
		
		# initiate a read-only connection to the input file
		incon <- file( txt_file , "r")

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
		hh.h <- read.table( hh.tfs[3], header = TRUE , sep = '\t' , na.strings = "NA" )
		
		# unique(sapply( hh.h , dbDataType , dbObj = db ))
		
		# count the number of records in each file
		hh.lines <- sapply( hh.tfs , R.utils::countLines )

		# read one of the person-level files into RAM..
		person.h <- read.table( person.tfs[3], header = TRUE , sep = '\t' , na.strings = "NA" )
		
		# unique(sapply( person.h , dbDataType , dbObj = db ))
		
		# count the number of records in each file
		person.lines <- sapply( person.tfs , R.utils::countLines )

		# use the monet.read.tsv function
		# to read the household files into a table called `hh.tn` in the monet database
		monet.read.tsv(
			db ,
			hh.tfs ,
			hh.tn ,
			nrows = hh.lines ,
			structure = hh.h ,
			nrow.check = 10000 ,
			lower.case.names = TRUE
		)

		# use the monet.read.tsv function
		# to read the household files into a table called `hh.tn` in the monet database
		monet.read.tsv(
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
				"as b on a.fileno = b.fileno AND a.serialno = b.serialno WITH DATA" 
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

		print( paste( merged.tn , "created!" ) )
		

		# add a column containing all ones to the current table
		DBI::dbSendQuery( db , paste0( 'alter table ' , merged.tn , ' add column one int' ) )
		DBI::dbSendQuery( db , paste0( 'UPDATE ' , merged.tn , ' SET one = 1' ) )
		
		
		# create a survey complex sample design object
		pums.design <-
			survey::svydesign(
				weight = if( grepl( "1990" , merged.tn ) ) ~pwgt1 else ~pweight ,			# weight variable column
				id = ~1 ,					# sampling unit column (defined in the character string above)
				data = merged.tn ,			# table name within the monet database (defined in the character string above)
				dbtype = "MonetDBLite" ,
				dbname = DBI::dbGetInfo( db )$gdk_dbpath
			)
		# ..and return that at the end of the function.
		pums.design
	}
