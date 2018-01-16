get_catalog_piaac <-
	function( data_name = "piaac" , output_dir , ... ){

		# designate the oecd public use file page
		oecd.csv.website <- 'http://vs-web-fs-1.oecd.org/piaac/puf-data/CSV/'

		# download the contents of that page
		csv.page <- readLines( oecd.csv.website , warn = FALSE )

		# figure out all lines on that page with a hyperlink
		csv.links <- unlist( strsplit( csv.page , "<A HREF=\"" ) )

		# further refine the links to only the ones containing the text `CSV/[something].csv`
		csv.texts <- csv.links[ grep( "(.*)CSV/(.*)\\.csv\">(.*)" , csv.links ) ]

		# figure out the base filename of each csv on the website
		csv.fns <- gsub( "(.*)CSV/(.*)\\.csv\">(.*)" , "\\2" , csv.texts )

		catalog <-
			data.frame(
				output_filename = paste0( output_dir , "/" , csv.fns , ".rds" ) ,
				design_filename = paste0( output_dir , "/" , csv.fns , " design.rds" ) ,
				full_url = paste0( oecd.csv.website , csv.fns , ".csv" ) ,
				stringsAsFactors = FALSE
			)

		catalog

	}


lodown_piaac <-
	function( data_name = "piaac" , catalog , ... ){

		on.exit( print( catalog ) )

		if ( !requireNamespace( "mitools" , quietly = TRUE ) ) stop( "mitools needed for this function to work. to install it, type `install.packages( 'mitools' )`" , call. = FALSE )

		tf <- tempfile()

		pvals <- c( 'pvlit' , 'pvnum' , 'pvpsl' )

		for ( i in seq_len( nrow( catalog ) ) ){

			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

			# import the csv file into working memory
			x <- read.csv( tf , stringsAsFactors = FALSE )

			# convert all column names to lowercase
			names( x ) <- tolower( names( x ) )
				
			# paste together all of the plausible value variables with the numbers 1 through 10
			pvars <- outer( pvals , 1:10 , paste0 ) 

			# figure out which variables in the `x` data.frame object
			# are not plausible value columns
			non.pvals <- names( x )[ !( names( x ) %in% pvars ) ]

			
			# loop through each of the ten plausible values..
			for ( k in 1:10 ){

				# create a new `y` data.frame object containing only the
				# _current_ plausible value variable (for example: `pvlit4` and `pvnum4` and `pvpsl4`)
				# and also all of the columns that are not plausible value columns
				y <- x[ , c( non.pvals , paste0( pvals , k ) ) ]

				# inside of that loop..
				# loop through each of the plausible value variables
				for ( j in pvals ){
					
					# within this specific `y` data.frame object
					
					# get rid of the number on the end, so
					# first copy the `pvlit4` to `pvlit` etc. etc.
					y[ , j ] <- y[ , paste0( j , k ) ]
					
					# then delete the `pvlit4` variable etc. etc.
					y[ , paste0( j , k ) ] <- NULL
					
				}
				
				# save the current `y` data.frame object as `x#` instead.
				if( k == 1 ) w <- list( y ) else w <- c( w , list( y ) )
				
			}

				
			# note: the piaac requires different survey designs for different countries.  quoting their technical documentation:
			# "The variable VEMETHOD denoting whether it is the JK1 or JK2 formula that is applicable to different countries must be in the dataset"

			# figure out jackknife method to use from the original `x` data.frame object
			
			# determine the unique values of the `vemethod` column in the current data.frame object
			jk.method <- unique( x$vemethod )
			
			# confirm that they are all the same value.  if there are more than one unique values, this line will crash the program.
			stopifnot( length( jk.method ) == 1 )
			
			# confirm that the jackknife method is one of these.  if it's not, again, crash the program.
			stopifnot( jk.method %in% c( 'JK1' , 'JK2' ) )
			
			# where oecd statisticians say `JK2` the survey package needs a `JKn` instead
			if ( jk.method == 'JK2' ) jk.method <- 'JKn'

			# construct the full multiply-imputed, replicate-weighted, complex-sample survey design object
			this_design <-
				survey::svrepdesign( 	
					weights = ~spfwt0 , 
					repweights = "spfwt[1-9]" ,
					rscales = rep( 1 , 80 ) ,
					scale = ifelse( jk.method == 'JKn' , 1 , 79 / 80 ) ,
					type = jk.method ,
					data = mitools::imputationList( w ) ,
					mse = TRUE
				)

			catalog[ i , 'case_count' ] <- nrow( this_design )
				
			# save both objects as `.rds` files
			saveRDS( x , file = catalog[ i , 'output_filename' ] )
			
			saveRDS( this_design , file = catalog[ i , 'design_filename' ] )

			# delete the temporary files
			suppressWarnings( file.remove( tf ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

		}

		on.exit()
		
		catalog

	}

