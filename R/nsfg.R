get_catalog_nsfg <-
	function( data_name = "nsfg" , output_dir , ... ){

		# figure out all `.dat` files on the cdc's nsfg ftp site
		dat_dir <- "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/NSFG/"
		dat_ftp <- strsplit( RCurl::getURL( dat_dir , ssl.verifypeer = FALSE ) , "<br>" )[[1]]
		all_files <- gsub( '(.*)\\">(.*)<\\/A>$', "\\2" , dat_ftp )
		dat_files <- all_files[ grep( "\\.dat$" , tolower( all_files ) ) ]


		# figure out all `.sas` files on the cdc's nsfg ftp site
		sas_dir <- paste0( dat_dir , "sas/" )
		sas_ftp <- strsplit( RCurl::getURL( sas_dir , ssl.verifypeer = FALSE ) , "<br>" )[[1]]
		all_files <- gsub( '(.*)\\">(.*)<\\/A>$', "\\2" , sas_ftp )
		sas_files <- all_files[ grep( "\\.sas$" , tolower( all_files ) ) ]

		# but remove ValueLabel and VarLabel scripts
		sas_files <- sas_files[ !grepl( "ValueLabel|VarLabel" , sas_files ) ]

		# identify starting years
		sy <- unique( substr( sas_files , 1 , 4 ) )

		# remove dat files without a starting year
		dat_files <- dat_files[ substr( dat_files , 1 , 4 ) %in% sy ]

		# remove this one too
		dat_files <- dat_files[ dat_files != "2002curr_ins.dat" ]

		# find appropriate sas file for this dat_file
		tsf <- gsub( "Setup|File" , "Data" , gsub( "\\.sas|\\.SAS|Input" , "" , sas_files ) )

		# sometimes sas files and dat files do not have the exact same names..
		
		# if they do, use it
		match_attempt <- sapply( dat_files , function( s ) ifelse( length( which( gsub( "\\.dat" , "" , s ) == tsf ) ) == 0 , NA , which( gsub( "\\.dat" , "" , s ) == tsf ) ) )
			
		catalog <-
			data.frame(
				full_url = dat_files , 
				sas_ri = sas_files[ match_attempt ] ,
				stringsAsFactors = FALSE
			)
			
		catalog[ catalog$full_url == "1973NSFGData.dat" , 'sas_ri' ] <- "1973FemRespSetup.sas"
		
		catalog[ catalog$full_url %in% c( "2011_2015_FemaleWeight.dat" , "2011_2015_MaleWeight.dat" ) , 'sas_ri' ] <- "2011_2015_4YearWeightSetup.sas"
		
		catalog[ catalog$full_url %in% "2013_2017_2011_2017_Femwgt.dat" , 'sas_ri' ] <- "2013_2017_4Year_2011_2017_6Year_FemWgtSetup.sas"
		
		catalog[ catalog$full_url %in% "2013_2017_2011_2017_Malewgt.dat" , 'sas_ri' ] <- "2013_2017_4Year_2011_2017_6Year_MaleWgtSetup.sas"
		
		catalog[ is.na( catalog$sas_ri ) , 'sas_ri' ] <- sas_files[ match( gsub( "\\.dat" , "" , catalog[ is.na( catalog$sas_ri ) , 'full_url' ] ) , gsub( "Data" , "" , tsf ) ) ]
		
		catalog[ is.na( catalog$sas_ri ) , 'sas_ri' ] <- sas_files[ match( gsub( "\\.dat" , "" , catalog[ is.na( catalog$sas_ri ) , 'full_url' ] ) , gsub( "FemPreg" , "Preg" , tsf ) ) ]

		catalog <- catalog[ !( catalog$full_url %in% c( "1976NSFGData.dat" , "1982NSFGData.dat" ) ) , ]
		
		catalog$beginline <- 1

		catalog$output_filename <- paste0( output_dir , "/" , gsub( "\\.dat$" , ".rds" , basename( catalog$full_url ) ) )
		
		catalog <-
			rbind(
				catalog ,
				data.frame(
					full_url = c( "1976NSFGData.dat" , "1976NSFGData.dat" , "1982NSFGData.dat" , "1982NSFGData.dat" ) ,
					sas_ri = c( "1976PregSetup.sas" , "1976FemRespSetup.sas" , "1982PregSetup.sas" , "1982FemRespSetup.sas" ) ,
					beginline = c( 194 , 7515 , 1567 , 1 ) ,
					output_filename = paste0( output_dir , "/" , c( "1976FemPreg.rds" , "1976FemResp.rds" , "1982FemPreg.rds" , "1982FemResp.rds" ) )
				)
			)
			
		catalog <- catalog[ order( catalog$full_url ) , ]
		
		catalog$full_url <- paste0( dat_dir , catalog$full_url )
		
		catalog$sas_ri <- paste0( sas_dir , catalog$sas_ri )

		catalog

	}


lodown_nsfg <-
	function( data_name = "nsfg" , catalog , ... ){

		on.exit( print( catalog ) )

		tf <- tempfile() ; tf2 <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			cachaca( catalog[ i , "sas_ri" ] , tf2 , mode = 'wb' )
			
			this_sas <- file( tf2 , 'rb' , encoding = 'latin1' )
			sas_lines <- readLines( this_sas , encoding = 'latin1' )
			close( this_sas )
			writeLines( sas_lines , tf2 )
			
			if( grepl( "1976FemRespSetup.sas" , catalog[ i , "sas_ri" ] , fixed = TRUE ) ){
			
				# load this file into working memory
				sasc <- readLines( tf2 )
				
				sasc <- gsub( "        F6TWOCOL 576-577        F7 578-579              F8 580" , "                F7 578-579              F8 580" , sasc , fixed = TRUE )
				sasc <- gsub( "F48_TWOCOL 661-662      F49 663                 F50 664" , "F49 663                 F50 664" , sasc , fixed = TRUE )
				
				# save it back onto the disk
				writeLines( sasc , tf2 )
			
			}
			
			if( grepl( "1982PregSetup.sas" , catalog[ i , "sas_ri" ] , fixed = TRUE ) ){
			
				# load this file into working memory
				sasc <- readLines( tf2 )
				
				sasc <- gsub( "BPRec\t11-12" , "TOSS\t1-10\tBPRec\t11-12" , sasc )
				
				sasc <- gsub( "CASEID \t\t1494-1498" , "CASEID \t\t1494-1498 		REC_TYPE 1499-1500" , sasc )
				
				sasc <- gsub( "MAROUT\t321\t\tFMAROUT\t322" , "" , sasc )
				
				# save it back onto the disk
				writeLines( sasc , tf2 )
			
			}

			if( grepl( "2002HHvars.dat" , catalog[ i , "full_url" ] , fixed = TRUE ) ){
			
				sasc <-
					"input CASEID   1 - 12
						   R_SEX     13
						   HHFAMTYP  14
						   HHPARTYP  15
						   NCHILDHH  16
						   HHKIDTYP  17
						   CSPBBHH   18
						   CSPBSHH   19
						   CSPSBHH   20
						   CSPOKDHH  21
					;"
					
				# save it back onto the disk
				writeLines( sasc , tf2 )
			
			}

			
			if( grepl( "1982FemRespSetup.sas" , catalog[ i , "sas_ri" ] , fixed = TRUE ) ){
			
				# read a slice of this sas import script into RAM
				sasc <- readLines( tf2 )[ 4322:4583 ]
			
				sasc <- gsub( "Qtype\t\t11\t\tcmbirth\t\t12-15\t\t\tA1\t\t18" , "TOSS\t1-10\tQtype\t\t11\t\tcmbirth\t\t12-15\t\t\tA1\t\t18" , sasc )
				sasc <- gsub( "F22_1CM\t622-628" , "F22_1CM\t623-628" , sasc )
				sasc <- gsub( "FMARITAL\t1041" , "" , sasc )
				sasc <- gsub( "CSECNUM  \t1167-1168" , "" , sasc )
				sasc <- gsub( "AGEREMAR\t1265-1267\t\tAGEREMAR\t1284-1287" , "AGEREMAR\t1284-1287" , sasc )
				# get rid of the tab separators and collapse all strings together into one
				sasc <- paste( gsub( "\t" , " " , sasc ) , collapse = " " )
				
				# remove double and triple spaces
				while( grepl( "  " , sasc ) ) sasc <- gsub( "  " , " " , sasc )
				
				# coerce this messy thing into a data.frame of values
				sasc <- data.frame( t( matrix( strsplit( sasc , " " )[[ 1 ]] , 2 ) ) )
				
				# coerce every column to character
				sasc[ , ] <- sapply( sasc[ , ] , as.character )
				
				# construct an ordered column from `X2`
				sasc$sortnum <- gsub( "-(.*)" , "" , sasc$X2 )
				
				# sort the data.frame
				sasc <- sasc[ order( as.numeric( sasc$sortnum ) ) , ]
				
				# remove the column you've just made
				sasc$sortnum <- NULL
				
				# add `rec_type` at the bottom of the data.frame
				sasc <- rbind( sasc , data.frame( X1 = "REC_TYPE" , X2 = "1499-1500" ) )
				
				# write this big string back onto the local disk as if it were a sas import script
				writeLines( paste( 'input\n ' , paste( apply( sasc , 1 , paste , collapse = ' ' ) , collapse = ' ' ) , ';' ) , tf2 )

			}
			
			
			# figure out the column positions
			suppressWarnings( sasc <- SAScii::parse.SAScii( tf2 , beginline = catalog[ i , 'beginline' ] ) )

			# fix widths on 2011_2015 weights file
			if( grepl( "2011_2015_4YearWeightSetup.sas" , catalog[ i , "sas_ri" ] , fixed = TRUE ) ) sasc$width <- 12
			
			
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )
						
			# this particular file has no line endings
			if( grepl( "1988PregData|1988FemRespData" , catalog[ i , 'full_url' ] ) ){

				# start an empty object
				fwf88 <- NULL
				
				# initiate a file-read connection to the downloaded file
				conn <- file( tf , 'rb' )
				
				# read 3553 characters at a time (the actual line length of this file)
				# until you are out of lines
				while( length( data88 <- readChar( conn, 3553 ) ) ){

					# stack the lines on top of one another
					fwf88 <- c( fwf88 , data88 )

				}
				
				# terminate the file-read connection
				close( conn )
				
				# write the full ascii file back to the temporary file
				writeLines( fwf88 , tf )

			}
			
			# read in the fixed-width file..
			x <- 
				readr::read_fwf(
					# using the ftp filepath
					tf ,
					# using the parsed sas widths
					readr::fwf_widths( abs( sasc$width ) , col_names = ifelse( !is.na( sasc$varname ) , sasc[  , 'varname' ] , 'missing' ) ) ,
					# using the parsed sas column types
					col_types = paste0( ifelse( is.na( sasc$varname ) , "_" , ifelse( sasc$char , "c" , "d" ) ) , collapse = "" ) ,
					
					na = c( "NA" , "" , "." ) ,
					
					locale = readr::locale( decimal_mark = "." , grouping_mark = "," ) 
				)
				
			x <- data.frame( x )
			
			stopifnot( nrow( x ) == R.utils::countLines( tf ) )
			
			# convert all column names to lowercase
			names( x ) <- tolower( names( x ) )
			
			if( grepl( "1976PregSetup.sas" , catalog[ i , 'sas_ri' ] , fixed = TRUE ) ){
			
				names( x )[ names( x ) == 'rectype' ] <- 'rec_type'
				
				x <- x[ x[ , 'rec_type' ] >= 5 , ]
				
				x <- mvrf_nsfg( x , readLines( tf2 ) )
			
			} else if( grepl( "1976FemRespSetup.sas" , catalog[ i , 'sas_ri' ] , fixed = TRUE ) ){
			
				x <- x[ x[ , 'marstat' ] <= 4 , ]

				x <- mvrf_nsfg( x , readLines( tf2 ) )
			
			} else if( grepl( "1982PregSetup.sas" , catalog[ i , 'sas_ri' ] , fixed = TRUE ) ){
			
				x <- x[ x[ , 'rec_type' ] > 0 , ]
				
				x <- mvrf_nsfg( x , readLines( tf2 ) )
			
			} else if( grepl( "1982FemRespSetup.sas" , catalog[ i , 'sas_ri' ] , fixed = TRUE ) ){
				
				x <- x[ x[ , 'rec_type' ] == 0 , ]
				
				x <- mvrf_nsfg( x , readLines( tf2 ) )
			
			} else x <- mvrf_nsfg( x , readLines( tf2 ) )
			
			stopifnot( nrow ( x ) > 0 )

			catalog[ i , 'case_count' ] <- nrow( x )
			
			# save this data.frame object to the local disk
			saveRDS( x , file = catalog[ i , "output_filename" ] , compress = FALSE )
			
			# delete the temporary files
			suppressWarnings( file.remove( tf ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

		}

		on.exit()
		
		catalog

	}


# define the missing value recode function
mvrf_nsfg <-
	function( x , sf ){
	
		# search within a `sas` import script for all `if` blocks ending with a .;
		mvr <- grep( "^if(.*)\\.;$" , tolower( stringr::str_trim( sf ) ) , value = TRUE )
		
		# loop through each one..
		for( this_mv in mvr ){
		
			# figure out the if block of the sas line
			ifs <- gsub( "if(.*)then(.*)=(.*)" , "\\1" , this_mv )
			# figure out the `then` bloc,
			thens <- stringr::str_trim( gsub( "if(.*)then(.*)=(.*)" , "\\2" , this_mv ) )
			
			# replace equalses, ors, and ands with their R representations
			ifs <- gsub( "=" , "%in%" , ifs )
			ifs <- gsub( " or " , "|" , ifs )
			ifs <- gsub( " and " , "&" , ifs )

			# overwrite records where the if statement is true with missing
			x[ with( x , which( eval( parse( text = ifs ) ) ) ) , thens ] <- NA
			
		}
		
		# return the missings-blanked-out data.frame object
		x
	}