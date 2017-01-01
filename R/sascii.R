


read_SAScii <-
	function( dat_path , sas_path , beginline = 1 , lrecl = NULL , skip_decimal_division = NULL , zipped = FALSE , ... ){

		sasc <- SAScii::parse.SAScii( sas_path , beginline = beginline , lrecl = lrecl )

		y <- sasc[ !is.na( sasc[ , "varname" ] ) , ]
		
		sasc$varname[ is.na( sasc$varname ) ] <- paste0( "toss" , seq( sum( is.na( sasc$varname ) ) ) )

		if (zipped) {
			tf <- tempfile()
			cachaca( dat_path , tf , mode = "wb" )
			dat_path <- unzip( tf , overwrite = TRUE )
			if( length( dat_path ) != 1 ) stop( "zipped file does not contain exactly one file" )
		}
		
		# read in the fixed-width file..
		x <-
			readr::read_fwf(
				# using the ftp filepath
				dat_path ,
				# using the parsed sas widths
				readr::fwf_widths( abs( sasc$width ) , col_names = sasc[ , 'varname' ] ) ,
				# using the parsed sas column types
				col_types = paste0( ifelse( grepl( "^toss" , sasc$varname ) , "_" , ifelse( sasc$char , "c" , "d" ) ) , collapse = "" ) ,
				# passed in from read_SAScii
				...
			)

		x <- data.frame( x )
				
		if (is.null(skip_decimal_division)) {
			
			user.defined.scipen <- getOption("scipen")
			
			options(scipen = 1e+06)
			
			no_decimal_points <- unlist( sapply( x , function( z ) ( sum( grepl( "." , z , fixed = TRUE ) ) == 0 ) ) )
			
			cols_to_multiply <- no_decimal_points & !y[ , "char" ] & y[ , "divisor" ] != 1
			
			x[ cols_to_multiply ] <- data.frame( t( t( x[ cols_to_multiply ] ) * y[ , "divisor" ] ) )
			
			options(scipen = user.defined.scipen)
			
		} else {
		
			if( !skip_decimal_division ){

				cols_to_multiply <- !y[ , "char" ] & y[ , "divisor" ] != 1
			
				x[ cols_to_multiply ] <- data.frame( t( t( x[ cols_to_multiply ] ) * y[ , "divisor" ] ) )
			

			}
			
		}

		
		x

	}
	



read_SAScii_monetdb <-
	function( 
		# differences between parameters for read.SAScii() (from the R SAScii package)
		# and read.SAScii.monetdb() documented here --
		fn ,
		sas_ri = NULL , 
		beginline = 1 , 
		zipped = F , 
		lrecl = NULL , 
		skip_decimal_division = FALSE , # skipping decimal division defaults to FALSE for this function!
		tl = F ,						# convert all column names to lowercase?
		tablename ,
		overwrite = FALSE ,				# overwrite existing table?
		connection ,
		tf.path = NULL ,				# do temporary files need to be stored in a specific folder?
										# this option is useful for keeping protected data off of random temporary folders on your computer--
										# specifying this option creates the temporary file inside the folder specified
		try_best_effort = FALSE ,
		sas_stru = NULL ,
		allow_zero_records = FALSE ,	# by default, expect more than zero records to be imported.
		na_strings = ""					# by default, na strings are empty
	) {
		if( is.null( sas_ri ) & is.null( sas_stru ) ) stop( "either sas_ri= or sas_stru= must be specified" )
		if( !is.null( sas_ri ) & !is.null( sas_stru ) ) stop( "either sas_ri= or sas_stru= must be specified, but not both" )

		if( length( na_strings ) != 1 ) stop( "na_strings must have length of one" )
	
	# before anything else, create the temporary files needed for this function to run
	# if the user doesn't specify that the temporary files get stored in a temporary directory
	# just put them anywhere..
	if ( is.null( tf.path ) ){
		tf <- tempfile()
		td <- tempdir()
		tf2 <- tempfile() 
	} else {
		# otherwise, put them in the protected folder
		tf.path <- normalizePath( tf.path )
		td <- tf.path
		tf <- paste0( tf.path , "/" , tablename , "1" )
		tf2 <- paste0( tf.path , "/" , tablename , "2" )
	}
	
	file.create( tf , tf2 )
	
	
	# scientific notation contains a decimal point when converted to a character string..
	# so store the user's current value and get rid of it.
	user.defined.scipen <- getOption( 'scipen' )
	
	# set scientific notation to something impossibly high.  Inf doesn't work.
	options( scipen = 1000000 )
	on.exit( options( scipen = user.defined.scipen ) )
	

	if( !is.null( sas_ri ) ){
	
		tf_sri <- tempfile()
		
		# if the sas read-in file needs to be downloaded..
		if( any( grepl( 'url' , attr( file( sas_ri ) , "class" ) ) ) ){
		
			# download it.
			cachaca( sas_ri , tf_sri , mode = 'wb' )
		
		# otherwise, just copy it over.
		} else tf_sri <- sas_ri
		
		x <- SAScii::parse.SAScii( tf_sri , beginline , lrecl )
	
	} else {
	
		x <- sas_stru
		
	}
	
	
	if( tl ) x$varname <- tolower( x$varname )
	
	#only the width field should include negatives
	y <- x[ !is.na( x[ , 'varname' ] ) , ]
	
	
	# deal with gaps in the data frame #
	num.gaps <- nrow( x ) - nrow( y )
	
	# if there are any gaps..
	if ( num.gaps > 0 ){
	
		# read them in as simple character strings
		x[ is.na( x[ , 'varname' ] ) , 'char' ] <- TRUE
		x[ is.na( x[ , 'varname' ] ) , 'divisor' ] <- 1
		
		# invert their widths
		x[ is.na( x[ , 'varname' ] ) , 'width' ] <- abs( x[ is.na( x[ , 'varname' ] ) , 'width' ] )
		
		# name them toss_1 thru toss_###
		x[ is.na( x[ , 'varname' ] ) , 'varname' ] <- paste( 'toss' , 1:num.gaps , sep = "_" )
		
		# and re-create y
		y <- x
	}
		
	#if the ASCII file is stored in an archive, unpack it to a temporary file and run that through read.fwf instead.
	if ( zipped ){
		#download the CPS repwgts zipped file
		cachaca( fn , tf , mode = "wb" )
		#unzip the file's contents and store the file name within the temporary directory
		fn <- unzip( tf , exdir = td , overwrite = T )
		
		on.exit( file.remove( tf ) )
	}

	
	# if the overwrite flag is TRUE, then check if the table is in the database..
	if ( overwrite ){
		# and if it is, remove it.
		if ( tablename %in% DBI::dbListTables( connection ) ) DBI::dbRemoveTable( connection , tablename )
		
		# if the overwrite flag is false
		# but the table exists in the database..
	} else {
		if ( tablename %in% DBI::dbListTables( connection ) ) stop( "table with this name already in database" )
	}
	
	for ( j in y$varname[ toupper( y$varname ) %in% getFromNamespace( "reserved_monetdb_keywords" , "MonetDBLite" ) ] ){
	
		print( paste0( 'warning: variable named ' , j , ' not allowed in monetdb' ) )
		print( paste0( 'changing column name to ' , j , '_' ) )
		y[ y$varname == j , 'varname' ] <- paste0( j , "_" )

	}

	fields <- y$varname

	colTypes <- ifelse( !y[ , 'char' ] , 'DOUBLE PRECISION' , 'STRING' )

	colDecl <- paste( fields , colTypes )

	sql.create <-
		sprintf(
			paste(
				"CREATE TABLE" ,
				tablename ,
				"(%s)"
			) ,
			paste(
				colDecl ,
				collapse = ", "
			)
		)
	
	# starts and ends
	w <- abs ( x$width )
		
	# in speed tests, adding the exact number of lines in the file was much faster
	# than setting a very high number and letting it finish..

	full_table_editing_attempt <-
		try({
		
			# create the table in the database
			DBI::dbSendQuery( connection , sql.create )

			#############################
			# begin importation attempt #

			DBI::dbSendQuery(
				connection , 
				paste0(
					"COPY INTO " , 
					tablename , 
					" FROM '" , 
					normalizePath( fn ) , 
					"' NULL AS " ,
					paste0( "'" , na_strings , "'" ) ,
					" " ,
					if( try_best_effort ) " BEST EFFORT " ,
					" FWF (" , 
					paste0( w , collapse = ", " ) , 
					")" 
				)
			)

			# end importation attempt #
			###########################	


			# loop through all columns to:
			# convert to numeric where necessary
			# divide by the divisor whenever necessary
			for ( l in seq( nrow(y) ) ){

				if ( ( y[ l , "divisor" ] != 1 ) & !( y[ l , "char" ] )	) {

					sql <- paste( "UPDATE" , tablename , "SET" , y[ l , 'varname' ] , "=" , y[ l , 'varname' ] , "*" , y[ l , "divisor" ] )

					if ( !skip_decimal_division ) DBI::dbSendQuery( connection , sql )


				}

			}

			# eliminate gap variables.. loop through every gap
			if ( num.gaps > 0 ){
				
				for ( i in seq( num.gaps ) ) {

					# create a SQL query to drop these columns
					sql.drop <- paste0( "ALTER TABLE " , tablename , " DROP toss_" , i )

					# and drop them!
					DBI::dbSendQuery( connection , sql.drop )
				}
				
			}
			
		} , silent = TRUE )
	

	# reset scientific notation length
	options( scipen = user.defined.scipen )
	
	# if anything about the table creation/import/editing went wrong, then remove the table and return the error
	if( class( full_table_editing_attempt ) == 'try-error' ){
	
		try( DBI::dbRemoveTable( connection , tablename ) , silent = TRUE )
		
		stop( full_table_editing_attempt )
		
	} 
	
	num_recs <- DBI::dbGetQuery( connection , paste( "SELECT COUNT(*) FROM" , tablename ) )[ 1 , 1 ]
	
	if( num_recs == 0 && !allow_zero_records ) {
		DBI::dbRemoveTable( connection , tablename )
		stop( "imported table has zero records.  if this is expected, set allow_zero_records = TRUE" )
	}
	
	num_recs
	
}
