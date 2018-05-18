get_catalog_faers <-
	function( data_name = "faers" , output_dir , ... ){

		catalog <- NULL
		
		tf <- tempfile()
	
		# specify the homepage of the legacy fda quarterly data sets
		legacy.url <- "https://wayback.archive-it.org/7993/20170404211700/https:/www.fda.gov/Drugs/GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/ucm083765.htm"

		# specify the homepage of the faers quarterly data sets
		faers.url <- "https://www.fda.gov/Drugs/GuidanceComplianceRegulatoryInformation/Surveillance/AdverseDrugEffects/ucm082193.htm"

		# loop through two text strings: `faers` and `legacy`
		for ( f.l in c( "faers" , "legacy" ) ){

			download.file( get( paste0( f.l , ".url" ) ) , tf , mode = 'wb' )
		
			# for both, download the contents of the homepages
			doc <- XML::htmlParse( tf )

			# extract all possible link blocks from the current document
			possible.links <- XML::xpathSApply( doc , "//a" , XML::xmlAttrs )
			
			# for each of those links, extract the text contained inside the possible-link block
			possible.href.names <- XML::xpathSApply( doc , "//a" , XML::xmlValue )

			# isolate only the links that lead to a `.zip` file
			zip.locations <- unlist( lapply( possible.links , function( z ) grepl( '\\.zip$' , tolower( z["href"] ) ) ) )

			# subset the `possible.links` list to only zipped files
			links <- possible.links[ zip.locations ]

			# repeat that subset on the list of names
			link.names <- possible.href.names[ zip.locations ]

			# confirm that these two results have the same length
			stopifnot( length( links ) == length( link.names ) )

			# identify which of the link-names have the word `ascii` in them
			names.with.ascii <- unlist( lapply( link.names , function( z ) grepl( 'ascii' , tolower( z ) ) ) )

			# identify which of the link-names-with-ascii have a `href` tag in them, indicating an actual hyperlink
			ascii.links <- lapply( links[ names.with.ascii ] , function( z ) z[ "href" ] )

			# further limit the `link.names` object to only those with the text `ascii`
			ascii.names <- link.names[ names.with.ascii ]

			# extract the four-digit year from the remaining filenames
			ascii.years <- gsub( "(.*)ASCII ([0-9]*)q(.*)" , "\\2" , ascii.names )
			ascii.years <- gsub( "(.*)ASCII_([0-9]*)q(.*)" , "\\2" , ascii.years )
			ascii.years <- gsub( "(.*)ASCII ([0-9]*) Q(.*)" , "\\2" , ascii.years )
			ascii.years <- gsub( "(.*)ASCII_([0-9]*) Q(.*)" , "\\2" , ascii.years )
			ascii.years <- gsub( "(.*)ASCII_([0-9]*)Q(.*)" , "\\2" , ascii.years )

			# extract the one-digit quarter from the remaining filenames
			ascii.quarter <- gsub( "(.*)([0-9]*)q([0-9])(.*)" , "\\3" , ascii.names )
			ascii.quarter <- gsub( "(.*)([0-9]*)Q([0-9])(.*)" , "\\3" , ascii.quarter )
			ascii.quarter <- gsub( "(.*)([0-9]*) q([0-9])(.*)" , "\\3" , ascii.quarter )
			ascii.quarter <- gsub( "(.*)([0-9]*) Q([0-9])(.*)" , "\\3" , ascii.quarter )

			# confirm all years are 2004 or later
			stopifnot( ascii.years %in% 2004:3000 )

			# confirm all quarters are one through four
			stopifnot( ascii.quarter %in% 1:4 )
		
			catalog <-
				rbind(
					catalog ,
					data.frame(
						year = ascii.years ,
						quarter = ascii.quarter ,
						fl = f.l ,
						full_url =  paste0( if( f.l == 'legacy' ) "https://wayback.archive-it.org" else "https://www.fda.gov" , unlist( ascii.links ) ) ,
						output_folder = paste0( output_dir , "/" , ascii.years , " q" , ascii.quarter ) ,
						stringsAsFactors = FALSE
					)
				)
		}
		
		
		catalog

	}


lodown_faers <-
	function( data_name = "faers" , catalog , ... ){

		on.exit( print( catalog ) )

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){
		
			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

			unzipped_files <- unzip_warn_fail( tf , exdir = paste0( catalog[ i , "output_folder" ] , "/unzips" ) )

			# limit the character vector containing all files to only the ones ending in `.txt`
			text.files <- unzipped_files[ grep( "\\.txt$" , tolower( unzipped_files ) ) ]

			# stat and size files contain control counts, not actual microdata, throw them out
			text.files <- text.files[ !( substr( tolower( basename( text.files ) ) , 1 , 4 ) %in% c( 'size' , 'stat' ) ) ]

			# loop through each of the text files to import..
			for ( j in text.files ){

				# determine the tablename (the filename sans extension)
				tablename <- gsub( "\\.txt$" , "" , tolower( basename( j ) ) )

				# create another character string appending `.rds` to the end
				rds.filename <- paste0( catalog[ i , 'output_folder' ] , "/" , tablename , ".rds" )
				
				# check for missing dollar signs at the end of the first column
				first.100 <- unlist( lapply( gregexpr( "\\$" , readLines( j , n = 100 ) ) , length ) )
				
				# check if there are different numbers of columns in the first hundred lines of the text file
				if( length( unique( first.100 ) ) > 1 ){

					# load in the entire text file
					lines <- readLines( j )
					
					# tack a `$` onto the end of the first line
					lines[ 1 ] <- paste0( lines[ 1 ] , "$" )
					
					# overwrite the text file with the extra $ on the first line
					writeLines( lines , j )
				
				}
				
				# attempt to read the text file into the object `x` using `$` separators.
				# if the read-in fails, do not break the loop.  instead, store that error in a `wrap.failure` object
				wrap.failure <- try( x <- read.table( j , sep = "$" , header = TRUE , comment.char = "" , quote = "" ) , silent = TRUE )
				# if the read-in works, you'll have the data.frame object `x` to work with.  hooray!
				
				# so long as the `wrap.failure` object contains an error..
				while( class( wrap.failure ) == 'try-error' ){
				
					# record the wrap failure's location
					wfl <- as.numeric( gsub( "(.*)line ([0-9]*) did not have(.*)" , "\\2" , wrap.failure[1] ) )
				
					# if the wrap failure location doesn't contain a problem-line, break the whole program.
					if( is.na( wfl ) ) stop( "one.  not a wrap failure.  something else is wrong." ) else {

						# doubly-confirm that a wrap failure is happening.  check whether the number of $'s (the delimiter) varies on any lines.
						if ( length( unique( unlist( lapply( gregexpr( "\\$" , a <- readLines( j ) ) , length ) ) ) ) == 1 ){
							
							stop( "two.  not a wrap failure.  something else is wrong." )
							
						} else {
						
							# paste the line of the wrap failure and the line after together.
							a <- c( a[ seq( wfl ) ] , paste0( a[ wfl + 1 ] , a[ wfl + 2 ] ) , a[ seq( wfl + 3 , length( a ) ) ] )
						
							# overwrite the file on the disk
							writeLines( a , j )
							
							# now the `read.table` command should work properly.
							wrap.failure <- try( x <- read.table( j , sep = "$" , header = TRUE , comment.char = "" , quote = "" ) , silent = TRUE )
							# if this ever works, you'll have `x` read in as a data.frame object.  hooray!
						
						}
						
					}
					
				}
				
				
				# determine which columns (if any) are 100% missings
				columns.100pct.missing <- names( which( sapply( x , function( z ) all( is.na( z ) ) ) ) )
				
				# just throw them out of the data.frame object
				x <- x[ , !( names( x ) %in% columns.100pct.missing ) ]
				
				# convert all column names to lowercase
				names( x ) <- tolower( names( x ) )
				
				# a primaryid column has extra characters in front that it shouldn't
				names( x )[ grepl( 'primaryid' , names( x ) ) ] <- 'primaryid'
				
				# a outc_code column occasionally is missing its final `e`
				names( x )[ names( x ) == 'outc_cod' ] <- 'outc_code'
				
				# `lot_num` and `lot_nbr` switch back and forth a lot.  pick one
				names( x )[ names( x ) == 'lot_num' ] <- 'lot_nbr'
				
				# save the data.frame object to the rds filename on the local disk
				saveRDS( x , file = rds.filename , compress = FALSE )
				
				catalog[ i , 'case_count' ] <- max( catalog[ i , 'case_count' ] , nrow( x ) , na.rm = TRUE )
				
			}

			# delete the temporary files
			suppressWarnings( file.remove( tf ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'output_folder' ] , "'\r\n\n" ) )

		}

		on.exit()
		
		catalog

	}

