get_catalog_cpsbasic <-
	function( data_name = "cpsbasic" , output_dir , ... ){

		cps_ftp <- "https://thedataweb.rm.census.gov/ftp/cps_ftp.html"
		
		link_page <- rvest::html_nodes( xml2::read_html( cps_ftp ) , "a" )
		
		link_text <- rvest::html_text( link_page )
		
		link_refs <- rvest::html_attr( link_page , "href" )
		
		link_refs <- gsub( "http://" , "https://" , link_refs , fixed = TRUE )
		
		cps_table <- rvest::html_table( xml2::read_html( cps_ftp ) , fill = TRUE )[[2]]
		
		basic_text <- link_text[ grep( "/basic/" , link_refs ) ]
		
		basic_refs <- link_refs[ grep( "/basic/" , link_refs ) ]
		
		zips <- grep( "\\.zip$" , basic_refs , value = TRUE , ignore.case = TRUE )
		
		# hardcoded exclusions
		zips <- zips[ !( zips %in% c( "https://thedataweb.rm.census.gov/pub/cps/basic/200701-/dec07revwgts.zip" , "https://thedataweb.rm.census.gov/pub/cps/basic/199801-/pubuse2000_2002.tar.zip" , "https://thedataweb.rm.census.gov/pub/cps/basic/200701-/disability.zip" ) ) ]
		
		cps_version <- gsub( "(.*)/(.*)" , "\\2" , dirname( zips ) )
		
		year_string <- as.numeric( gsub( "([A-z]+)([0-9]+)pub\\.zip" , "\\2" , basename( zips ) ) )
		
		year_string <- ifelse( year_string > 93 , year_string + 1900 , year_string + 2000 )
		
		# unique_versions <- unique( cps_version )
		
		possible_dds <- grep( "\\.txt$|\\.asc$" , basic_refs , ignore.case = TRUE , value = TRUE )
		
		# hardcoded exclusion
		possible_dds <- possible_dds[ !grepl( "dec07revwgts_dd\\.txt|2000\\-2extract\\.txt" , possible_dds ) ]

		this_dd <- mapply( rep , possible_dds , rle( cps_version )$length )

		catalog <-
			data.frame(
				year = year_string ,
				month = match( substr( basename( zips ) , 1 , 3 ) , tolower( month.abb ) ) ,
				dd = unlist( this_dd ) ,
				version = cps_version ,
				full_url = zips ,
				stringsAsFactors = FALSE
			)
			
		# hardcoded fix
		catalog[ catalog$year == "2017" & catalog$month %in% 1:5 , "dd" ] <- "https://thedataweb.rm.census.gov/pub/cps/basic/201701-/January_2017_Record_Layout.txt"

		catalog$output_filename = paste0( output_dir , "/" , catalog$year , " " , stringr::str_pad( catalog$month , 2 , pad = "0" ) , " cps basic.rds" )
		
		rownames( catalog ) <- NULL

		catalog <- catalog[ order( catalog$year , catalog$month ) , ]
		
		catalog

	}


lodown_cpsbasic <-
	function( data_name = "cpsbasic" , catalog , ... ){

		on.exit( print( catalog ) )

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

			unzipped_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , "/unzips" ) )

			sas_lines <- cps_dd_parser( catalog[ i , 'dd' ] )
			
			x <- read_SAScii( unzipped_files , sas_stru = sas_lines , na_values = c( "NA" , "" , "." , "*" , "-" ) )

			# convert all column names to lowercase
			names( x ) <- tolower( names( x ) )

			catalog[ i , 'case_count' ] <- nrow( x )
			
			saveRDS( x , file = catalog[ i , 'output_filename' ] , compress = FALSE ) ; rm( x ) ; gc()

			# delete the temporary files
			suppressWarnings( file.remove( tf , unzipped_files ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

		}

		on.exit()
		
		catalog

	}


		
# data dictionary parser
cps_dd_parser <-
	function( dd_url ){

		# read in the data dictionary
		tf <- tempfile()
		httr::GET( dd_url , httr::write_disk( tf , overwrite = TRUE ) )
		dd_con <- file( tf , 'rb' , encoding = 'latin1' )
		the_lines <- readLines( dd_con , encoding = 'latin1' )
		close( dd_con )
		
		the_lines <- gsub( "\u0096" , "-" , the_lines )
		the_lines <- gsub( "\\u0096" , "-" , the_lines )
		the_lines <- gsub( "\\\u0096" , "-" , the_lines )
		the_lines <- gsub("\u0085", "X", the_lines)
		the_lines <- gsub("\\u0085", "X", the_lines)
		the_lines <- gsub("\\\u0085", "X", the_lines)
		the_lines <- gsub("\u0092", "X", the_lines)
		the_lines <- gsub("\\u0092", "X", the_lines)
		the_lines <- gsub("\\\u0092", "X", the_lines)
		the_lines <- gsub( "\\(|\\)" , "" , the_lines )
		
		# hardcodes
		
		the_lines <- gsub( "794 - 680" , "679 - 680" , the_lines )
		
		if( dd_url %in% c( "https://thedataweb.rm.census.gov/pub/cps/basic/200701-/jan07dd.txt" , "https://thedataweb.rm.census.gov/pub/cps/basic/200508-/augnov05dd.txt" ) ){
			the_lines <- gsub( "PRNMCHLD" , "PRNMCHLD 2" , the_lines )
			the_lines <- gsub( "PEHGCOMP" , "PEHGCOMP 2" , the_lines )
			the_lines <- gsub( "HURHHSCRN*" , "HURHHSCRN" , the_lines , fixed = TRUE ) 
			the_lines <- gsub( "PURKAT1*" , "PURKAT1" , the_lines , fixed = TRUE ) 
			the_lines <- gsub( "PURKAT2*" , "PURKAT2" , the_lines , fixed = TRUE ) 
		}
		
		if( dd_url == "https://thedataweb.rm.census.gov/pub/cps/basic/200508-/augnov05dd.txt" ){
		
			the_lines <- the_lines[ !grepl( "FILLER          1      August - October 2005 Only                       886-886" , the_lines , fixed = TRUE ) ]
		
		}
		
		if( dd_url == "https://thedataweb.rm.census.gov/pub/cps/basic/200405-/may04dd.txt" ){
		
			the_lines <- gsub( "HRHHID partII 5" , "HRHHID 5" , the_lines )
			the_lines <- gsub( "411 - 412" , "410 - 411" , the_lines )
			the_lines <- gsub( "PEHGCOMP" , "PEHGCOMP 2" , the_lines )
			
		}
		
		
		if( dd_url == "https://thedataweb.rm.census.gov/pub/cps/basic/200301-/jan03dd.txt" ){
		
			the_lines <- gsub( "PULKPS4      2     SAME AS PULKPS2 FOURTH METHOD" , "PULKPS4      2     SAME AS PULKPS2 FOURTH METHOD 326 - 327" , the_lines )
			the_lines <- gsub( "PRNMCHLD" , "PRNMCHLD 2" , the_lines )
			the_lines <- gsub( "PEHGCOMP" , "PEHGCOMP 2" , the_lines )
		
		}
		
		
		if( dd_url %in% c( "https://thedataweb.rm.census.gov/pub/cps/basic/199506-199508/jun95_aug95_dd.txt" , "https://thedataweb.rm.census.gov/pub/cps/basic/199404-199505/apr94_may95_dd.txt" ,  "https://thedataweb.rm.census.gov/pub/cps/basic/199401-199403/jan94_mar94_dd.txt" ) ){
		
			the_lines <- gsub( "PEAFNOW      2     ARE YOU NOW IN THE ARMED FORCES      134 - 136" , "PEAFNOW      2     ARE YOU NOW IN THE ARMED FORCES      135 - 136" , the_lines )
			
		}
		
		if( dd_url == "https://thedataweb.rm.census.gov/pub/cps/basic/199801-/2000-2extract.txt" ){
		
			the_lines <- the_lines[ !grepl( "PADDING      4     January 2000 - December 2000 Only                         116-119" , the_lines ) ]
		
		}

		if( dd_url == "https://thedataweb.rm.census.gov/pub/cps/basic/199801-/jan98dd.asc" ){
		
			# find lines with "implied" after break
			idx_implied <- grep( "^[ ]+implied", the_lines, ignore.case = TRUE)
			
			# copy string to previous line
			the_lines[idx_implied-1] <- paste(the_lines[idx_implied-1],gsub("^[ ]+"," ",the_lines[idx_implied]))
			
			the_lines[idx_implied] <- ""
			
			the_lines[759] <- "D FILLER 2 149"
			
			the_lines[3132] <- "D FILLER 4 536"
			
			the_lines[3164] <- "D FILLER 2 557"
			
			the_lines[3315] <- "D FILLER 6 633"
			
			the_lines[3431] <- "D FILLER 2 681"
			
			the_lines[3737] <- "D FILLER 4 787"
			
		}

		
		# end of hardcodes
		
		# pull the lines into a temporary variable
		the_dd <- stringr::str_trim( the_lines )
	
		the_dd <- iconv( the_dd , "" , "ASCII//TRANSLIT" )
		
		the_dd <- gsub( 'a?\"' , '-' , the_dd , fixed = TRUE )
	
		# remove any goofy tab characters
		the_dd <- gsub( "\t" , " " , the_dd )
		
		# look for lines indicating divisor
		idp <- grep( "([0-9]+) implied" , the_dd , ignore.case = TRUE )
		
		decimal_lines <- gsub( "[^0-9]" , "" , the_dd[idp] )
		
		# keep only the variable lines
		rows_to_keep <- grep( "^([A-Z])(.*)([0-9])$" , the_dd )
		
		for( this_line in seq_along( idp ) ) while( !( idp[ this_line ] %in% rows_to_keep ) ) idp[ this_line ] <- idp[ this_line ] - 1
		
		the_dd <- the_dd[ rows_to_keep ] ; idp <- match( idp , rows_to_keep )
		
		
		
		rows_to_keep <- which( !grepl( "^EDITED" , the_dd , ignore.case = TRUE ) )
		
		for( this_line in seq_along( idp ) ) while( !( idp[ this_line ] %in% rows_to_keep ) ) idp[ this_line ] <- idp[ this_line ] - 1
		
		the_dd <- the_dd[ rows_to_keep ] ; idp <- match( idp , rows_to_keep )
		
		
		rows_to_keep <- which( !grepl( "^REVISED" , the_dd , ignore.case = TRUE ) )
		
		for( this_line in seq_along( idp ) ) while( !( idp[ this_line ] %in% rows_to_keep ) ) idp[ this_line ] <- idp[ this_line ] - 1
		
		the_dd <- the_dd[ rows_to_keep ] ; idp <- match( idp , rows_to_keep )
		
		
		rows_to_keep <- which( !grepl( "^STARTING" , the_dd , ignore.case = TRUE ) )
		
		for( this_line in seq_along( idp ) ) while( !( idp[ this_line ] %in% rows_to_keep ) ) idp[ this_line ] <- idp[ this_line ] - 1
		
		the_dd <- the_dd[ rows_to_keep ] ; idp <- match( idp , rows_to_keep )
		
		
		
		the_dd <- gsub( "( +)-( +)" , "-" , the_dd )
		
		the_dd <- gsub( "-( +)" , "-" , the_dd )
		
		the_dd <- gsub( "( +)-" , "-" , the_dd )

		the_dd <- gsub( "( +)" , " " , the_dd )
		
		the_dd <- gsub("\u0085", "X", the_dd)
		
		the_dd <- gsub("\\u0085", "X", the_dd)
		
		the_dd <- gsub("\\\u0085", "X", the_dd)

		the_dd <- gsub("\u0092", "X", the_dd)
		
		the_dd <- gsub("\\u0092", "X", the_dd)
		
		the_dd <- gsub("\\\u0092", "X", the_dd)
	
		the_dd <- gsub( "^FILLER ([0-9]+) ([0-9]+)-([0-9]+)$" , "FILLER \\1 FILLER \\2-\\3" , the_dd )
	
		the_dd <- gsub( "^PADDING ([0-9]+) ([0-9]+)-([0-9]+)$" , "PADDING \\1 PADDING \\2-\\3" , the_dd )
	
		if( dd_url == "https://thedataweb.rm.census.gov/pub/cps/basic/199801-/jan98dd.asc" ){
		
			the_dd <- gsub( "^D " , "" , the_dd )
			
			the_dd <- 
				paste( 
					the_dd , 
					as.numeric( gsub( "(.*) (.*) (.*)" , "\\3" , the_dd ) ) + 
					as.numeric( gsub( "(.*) (.*) (.*)" , "\\2" , the_dd ) ) - 1 
				)

		} else {
		
			# keep only the first three items in the line
			the_dd <- gsub( "([A-z0-9]+) ([0-9]+) (.*) ([0-9]+)-([0-9]+)" , "\\1 \\2 \\4 \\5" , the_dd )
		
		}
		
		rows_to_keep <- grep( "([A-z0-9]+) ([0-9]+) ([0-9]+) ([0-9]+)" , the_dd )
		
		for( this_line in seq_along( idp ) ) while( !( idp[ this_line ] %in% rows_to_keep ) ) idp[ this_line ] <- idp[ this_line ] - 1
		
		the_dd <- the_dd[ rows_to_keep ] ; idp <- match( idp , rows_to_keep )
		
		# hardcoded removals
		if( dd_url %in% c( "https://thedataweb.rm.census.gov/pub/cps/basic/201205-/may12dd.txt" , "https://thedataweb.rm.census.gov/pub/cps/basic/201001-/jan10dd.txt" ,  "https://thedataweb.rm.census.gov/pub/cps/basic/200901-/jan09dd.txt" , "https://thedataweb.rm.census.gov/pub/cps/basic/200701-/jan07dd.txt" , "https://thedataweb.rm.census.gov/pub/cps/basic/200508-/augnov05dd.txt" , "https://thedataweb.rm.census.gov/pub/cps/basic/200405-/may04dd.txt" ) ){
		
			the_dd <- the_dd[ !grepl( "^PROLDRRP" , the_dd ) ]
			
		}
		
	
		# break the lines apart by spacing
		the_dd <- strsplit( the_dd , " " )
		
		# store the variable name, width, and position into a data.frame
		the_result <-
			data.frame( 
				varname = sapply( the_dd , '[[' , 1 ) ,
				width = as.numeric( sapply( the_dd , '[[' , 2 ) ) ,
				start_position = as.numeric( sapply( the_dd , '[[' , 3 ) ) , 
				end_position = as.numeric( sapply( the_dd , '[[' , 4 ) ) ,
				divisor = 1 ,
				stringsAsFactors = FALSE
			)
			
		the_result[ idp , 'divisor' ] <- 10^-as.numeric( decimal_lines )
	
		for( i in seq( 2 , nrow( the_result ) ) ) stopifnot( the_result[ i , 'start_position' ] == the_result[ i - 1 , 'end_position' ] + 1 )
	
		the_result$width <- the_result$end_position - the_result$start_position + 1
	
		# fillers should be missings not 
		the_result[ the_result$varname == 'FILLER' , 'width' ] <- -( the_result[ the_result$varname == 'FILLER' , 'width' ] )
	
		the_result[ the_result$varname == 'FILLER' , 'varname' ] <- NA
		
		# treat cps fields as exclusively numeric
		the_result$char <- FALSE
			
		the_result
			
	}
	

