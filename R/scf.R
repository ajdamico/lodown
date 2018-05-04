get_catalog_scf <-
	function( data_name = "scf" , output_dir , ... ){

		base_url <- "https://www.federalreserve.gov/econres/files/"

		latest_year <-
			max(
				as.numeric(
					gsub(
						"(.*)([0-9][0-9][0-9][0-9])(.*)" ,
						"\\2" ,
						grep(
							'scf([0-9][0-9][0-9][0-9])' ,
							readLines( "https://www.federalreserve.gov/econres/scfindex.htm" , warn = FALSE ) ,
							value = TRUE
						)
					)
				)
			)

		catalog <-
			data.frame(
				year =
					c( 1989 , 1992 , 1995 , 1998 , 2001 , 2004 , 2007 , 2009 , seq( 2010 , latest_year , 3 ) ) ,
				main_url =
					c( 'scf89s' , 'scf92s' , 'scf95s' , 'scf98s' , 'scf01s' , 'scf2004s' , 'scf2007s' , 'scf2009ps' , paste0( "scf" , seq( 2010 , latest_year , 3 ) , "s" ) ) ,
				extract_url =
					c( 'scfp1989s' , 'scfp1992s' , 'scfp1995s' , 'scfp1998s' , 'scfp2001s' , 'scfp2004s' , 'scfp2007s' , 'rscfp2009panels' , paste0( "scfp" , seq( 2010 , latest_year , 3 ) , "s" ) ) ,
				rw_url =
					c( 'scf89rw1s' , 'scf92rw1s' , 'scf95rw1s' , 'scf98rw1s' , 'scf2001rw1s' , 'scf2004rw1s' , 'scf2007rw1s' , 'scf2009prw1s' , paste0( "scf" , seq( 2010 , latest_year , 3 ) , "rw1s" ) ) ,
				stringsAsFactors = FALSE
			)

		catalog$output_filename <- paste0( output_dir , "/scf " , catalog$year , ifelse( catalog$year == 2009 , " panel" , "" ) , ".rds" )

		catalog$rw_filename <- paste0( output_dir , "/scf " , catalog$year , ifelse( catalog$year == 2009 , " panel" , "" ) , " rw.rds" )

		catalog[ c( 'main_url' , 'extract_url' , 'rw_url' ) ] <- sapply( catalog[ c( 'main_url' , 'extract_url' , 'rw_url' ) ] , function(z) paste0(base_url , z , '.zip') )

		catalog

	}


lodown_scf <-
	function( data_name = "scf" , catalog , ... ){

		on.exit( print( catalog ) )

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			cachaca( catalog[ i , "main_url" ] , tf , mode = 'wb' )
			scf.m <- data.frame( haven::read_dta( unzipped_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , '/unzips' ) ) ) )
			file.remove( unzipped_files )

			cachaca( catalog[ i , "extract_url" ] , tf , mode = 'wb' )
			scf.e <- data.frame( haven::read_dta( unzipped_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , '/unzips' ) ) ) )
			file.remove( unzipped_files )

			cachaca( catalog[ i , "rw_url" ] , tf , mode = 'wb' )
			rw <- data.frame( haven::read_dta( unzipped_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , '/unzips' ) ) ) )
			file.remove( unzipped_files )

			names( scf.m ) <- tolower( names( scf.m ) )
			names( scf.e ) <- tolower( names( scf.e ) )
			names( rw ) <- tolower( names( rw ) )


			# the number of rows in the main file should exactly equal
			# the number of rows in the extract file
			stopifnot( nrow( scf.m ) == nrow( scf.e ) )


			# the 2007 replicate weights file has a goofy extra record for some reason..
			# ..so delete it manually.
			# if the current year being downloaded is 2007,
			# then overwrite the replicate weights table (rw)
			# with the same table, but missing unique id 1817
			# (which is the non-matching record)
			if ( catalog[ i , 'year' ] == 2007 ) rw <- rw[ !( rw$yy1 == 1817 ) , ]


			# the number of rows in the main file should exactly equal
			# the number of rows in the replicate weights file, times five
			if( nrow( scf.m ) != ( nrow( rw ) * 5 ) ){
				print( "the number of records in the main file doesn't equal five times the number in the rw file" )
				print( paste( 'scf.m rows:' , nrow( scf.m ) , " / rw rows:" , nrow( rw ) ) )
				stop( "this must be fixed before continuing." )
			}

			# the 1989 files contain unique identifiers `x1` and `xx1`
			# instead of `y1` and `yy1` .. change those two columns in all three data files.
			if ( catalog[ i , 'year' ] == 1989 ){
				names( scf.m )[ names( scf.m ) == 'x1' ] <- 'y1' ; names( scf.m )[ names( scf.m ) == 'xx1' ] <- 'yy1' ;
				names( scf.e )[ names( scf.e ) == 'x1' ] <- 'y1' ; names( scf.e )[ names( scf.e ) == 'xx1' ] <- 'yy1'
				names( rw )[ names( rw ) == 'x1' ] <- 'y1' ; names( rw )[ names( rw ) == 'xx1' ] <- 'yy1'
			}

			# confirm that the only overlapping columns
			# between the three data sets are `y1`
			# (the unique primary economic unit id - peu)
			# and `yy1` (the five records of the peu)
			stopifnot( all.equal( sort( intersect( names( scf.m ) , names( scf.e ) ) ) , c( 'y1' , 'yy1' ) ) )
			stopifnot( all.equal( sort( intersect( names( scf.m ) , names( rw ) ) ) , c( 'y1' , 'yy1' ) ) )
			stopifnot( all.equal( sort( intersect( names( scf.e ) , names( rw ) ) ) , c( 'y1' , 'yy1' ) ) )

			# throw out the unique identifiers ending with `1`
			# because they only match one-fifth of the records in the survey data
			rw$y1 <- NULL

			# `scf.m` currently contains
			# five records per household -- all five of the implicates.

			# add a column `one` to every record, containing just the number one
			scf.m$one <- 1

			# add a column `five` to every record, containing just the number five
			scf.m$five <- 5
			# note: this column should be used to calculate weighted totals.

			# break `scf.m` into five different data sets
			# based on the final character of the column 'y1'
			# which separates the five implicates
			scf.1 <- scf.m[ substr( scf.m$y1 , nchar( scf.m$y1 ) , nchar( scf.m$y1 ) ) == 1 , ]
			scf.2 <- scf.m[ substr( scf.m$y1 , nchar( scf.m$y1 ) , nchar( scf.m$y1 ) ) == 2 , ]
			scf.3 <- scf.m[ substr( scf.m$y1 , nchar( scf.m$y1 ) , nchar( scf.m$y1 ) ) == 3 , ]
			scf.4 <- scf.m[ substr( scf.m$y1 , nchar( scf.m$y1 ) , nchar( scf.m$y1 ) ) == 4 , ]
			scf.5 <- scf.m[ substr( scf.m$y1 , nchar( scf.m$y1 ) , nchar( scf.m$y1 ) ) == 5 , ]

			# count the total number of records in `scf.m`
			m.rows <- nrow( scf.m )

			# merge the contents of the extract data frames
			# to each of the five implicates
			imp1 <- merge( scf.1 , scf.e )
			imp2 <- merge( scf.2 , scf.e )
			imp3 <- merge( scf.3 , scf.e )
			imp4 <- merge( scf.4 , scf.e )
			imp5 <- merge( scf.5 , scf.e )

			rm( scf.1 , scf.2 , scf.3 , scf.4 , scf.5 , scf.m , scf.e ) ; gc()

			# confirm that the number of records did not change
			stopifnot(
				sum( nrow( imp1 ) , nrow( imp2 ) , nrow( imp3 ) , nrow( imp4 ) , nrow( imp5 ) ) == m.rows
			)

			# sort all five implicates by the unique identifier
			imp1 <- imp1[ order( imp1$yy1 ) , ]
			imp2 <- imp2[ order( imp2$yy1 ) , ]
			imp3 <- imp3[ order( imp3$yy1 ) , ]
			imp4 <- imp4[ order( imp4$yy1 ) , ]
			imp5 <- imp5[ order( imp5$yy1 ) , ]


			# replace all missing values in the replicate weights table with zeroes..
			rw[ is.na( rw ) ] <- 0

			# ..then multiply the replicate weights by the multiplication factor
			rw[ , paste0( 'wgt' , 1:999 ) ] <- rw[ , paste0( 'wt1b' , 1:999 ) ] * rw[ , paste0( 'mm' , 1:999 ) ]

			# only keep the unique identifier and the final (combined) replicate weights
			rw <- rw[ , c( 'yy1' , paste0( 'wgt' , 1:999 ) ) ]

			# sort the replicate weights data frame by the unique identifier as well
			rw <- rw[ order( rw$yy1 ) , ]

			# save the five implicates and the replicate weights file
			saveRDS( list( imp1 , imp2 , imp3 , imp4 , imp5 ) , file = catalog[ i , 'output_filename' ] , compress = FALSE )
			saveRDS( rw , file = catalog[ i , 'rw_filename' ] , compress = FALSE )

			catalog[ i , 'case_count' ] <- nrow( imp1 )

			rm( imp1 , imp2 , imp3 , imp4 , imp5 , rw ) ; gc()

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

		}

		# delete the temporary files
		file.remove( tf )

		on.exit()
		
		catalog

	}


	

#' variant of \code{mitools::MIcombine} that only uses the sampling variance from the first implicate instead of averaging all five
#'
#' @seealso \url{https://cran.r-project.org/web/packages/mitools/mitools.pdf}
#'
#' @rdname scf
#' @export
scf_MIcombine <-
	function (results, variances, call = sys.call(), df.complete = Inf, ...) {
		m <- length(results)
		oldcall <- attr(results, "call")
		if (missing(variances)) {
			variances <- suppressWarnings(lapply(results, vcov))
			results <- lapply(results, coef)
		}
		vbar <- variances[[1]]
		cbar <- results[[1]]
		for (i in 2:m) {
			cbar <- cbar + results[[i]]
			# MODIFICATION:
			# vbar <- vbar + variances[[i]]
		}
		cbar <- cbar/m
		# MODIFICATION:
		# vbar <- vbar/m
		evar <- var(do.call("rbind", results))
		r <- (1 + 1/m) * evar/vbar
		df <- (m - 1) * (1 + 1/r)^2
		if (is.matrix(df)) df <- diag(df)
		if (is.finite(df.complete)) {
			dfobs <- ((df.complete + 1)/(df.complete + 3)) * df.complete *
			vbar/(vbar + evar)
			if (is.matrix(dfobs)) dfobs <- diag(dfobs)
			df <- 1/(1/dfobs + 1/df)
		}
		if (is.matrix(r)) r <- diag(r)
		rval <- list(coefficients = cbar, variance = vbar + evar *
		(m + 1)/m, call = c(oldcall, call), nimp = m, df = df,
		missinfo = (r + 2/(df + 3))/(r + 1))
		class(rval) <- "MIresult"
		rval
	}


