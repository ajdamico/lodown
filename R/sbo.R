get_catalog_sbo <-
	function( data_name = "sbo" , output_dir , ... ){
		
		
	catalog <-
		data.frame(
			full_url = "http://www2.census.gov/econ/sbo/07/pums/pums_csv.zip" ,
			db_tablename = "x" ,
			dbfolder = paste0( output_dir , "/MonetDB" ) ,
			stringsAsFactors = FALSE
		)

	catalog

}


lodown_sbo <-
	function( data_name = "sbo" , catalog , ... ){
	
		if( nrow( catalog ) != 1 ) stop( "sbo catalog must be exactly one record" )
	
		tf <- tempfile()
	
		cachaca( catalog$full_url , tf , mode = 'wb' )
		
		unzipped_files <- unzip( tf , exdir = tempdir() )
	
		# connect to the MonetDBLite database (.db)
		db <- DBI::dbConnect( MonetDBLite::MonetDBLite() , catalog$dbfolder )

		# read the comma separated value (csv) file you just downloaded
		# directly into the monetdb database you just created.
		DBI::dbWriteTable( db , catalog$db_tablename , unzipped_files , sep = "," , header = TRUE , lower.case.names = TRUE )
		# yes.  you did all that.  nice work.

		# add a new numeric column called `one` to the `y` data table
		DBI::dbSendQuery( db , paste0( 'ALTER TABLE ' , catalog$db_tablename , ' ADD COLUMN one DOUBLE PRECISION' ) )
		# and fill it with all 1s for every single record.
		DBI::dbSendQuery( db , paste0( 'UPDATE ' , catalog$db_tablename , ' SET one = 1' ) )

		# add a new numeric column called `newwgt` to the `y` data table
		DBI::dbSendQuery( db , paste0( 'ALTER TABLE ' , catalog$db_tablename , ' ADD COLUMN newwgt DOUBLE PRECISION' ) )

		# and use the weights displayed in the census bureau's technical documentation
		DBI::dbSendQuery( db , paste0( 'UPDATE ' , catalog$db_tablename , ' SET newwgt = 10 * tabwgt * SQRT( 1 - 1 / tabwgt )' ) )
		# http://www2.census.gov/econ/sbo/07/pums/2007_sbo_pums_users_guide.pdf#page=7

		# disconnect from the current database
		DBI::dbDisconnect( db , shutdown = TRUE )

		# delete the temporary files
		suppressWarnings( file.remove( tf , unzipped_files ) )

		cat( paste0( data_name , " catalog entry " , 1 , " of " , nrow( catalog ) , " stored at '" , catalog$output_filename , "'\r\n\n" ) )

		invisible( TRUE )

	}


# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # functions related to survey statistical analysis  # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # #


# re-create the `with` function that looks for both
# the coefficient and the variance survey objects
with.sbosvyimputationList <-
	function ( sbo.svy , expr ){
	
		pf <- parent.frame()
		
		expr <- substitute( expr )
		
		expr$design <- as.name(".design")

		# this pulls in means, medians, totals, etc.
		# notice it uses sbo.svy$coef
		results <- eval( expr , list( .design = sbo.svy$coef ) )
		
		# this is used to calculate the variance, adjusted variance, standard error
		# notice it uses the sbo.svy$var object
		variances <- 
			lapply( 
				sbo.svy$var$designs , 
				function( .design ){ 
					eval( expr , list( .design = .design ) , enclos = pf ) 
				} 
			)
		
		# combine both results..
		rval <- list( coef = results , var = variances )
		
		# ..into a brand new object class
		class( rval ) <- 'sboimputationResultList'
		
		# and return it.
		rval
	}

# re-vamp the mitools package `MIcombine` function
# so it works on `rval` objects created by the `with` method above

# also note, the 2007-specific variance adjustment.  this will change in other years
# this adjustment statistic was pulled from the middle of page 8
# http://www2.census.gov/econ/sbo/07/pums/2007_sbo_pums_users_guide.pdf#page=8
MIcombine.sboimputationResultList <-
	function( x , adjustment = 1.992065 ){
	
		# just pull the structure of a variance-covariance matrix
		variance.shell <- suppressWarnings( vcov( x$var[[1]] ) )
		
		# initiate a function that will overwrite the diagonals.
		diag.replacement <-	
			function( z ){
				diag( variance.shell ) <- coef( z )
				variance.shell
			}
			
		# overwrite all the diagonals in the variance svy.sbo object
		coef.variances <- lapply( x$var , diag.replacement )
	
		# add 'em all together and divide by ten.
		midpoint <- Reduce( '+' , coef.variances ) / 10
	
		# initiate another function that takes some object,
		# subtracts the midpoint, squares it, and divides by ninety
		midpoint.var <- function( z ){ 1/10 * ( ( midpoint - z )^2 / 9 ) }
	
		# sum up all the differences into a single object
		variance <- Reduce( '+' , lapply( coef.variances , midpoint.var ) )
		
		# adjust every. single. number.
		adj_var <- adjustment * variance

		# construct a result that looks a lot like
		# other MIcombine methods.
		rval <-
			list( 
				coefficients = coef( x$coef ) ,
				variance = adj_var
			)
		
		# call it an MIresult class, just like all other MIcombine results.
		class( rval ) <- 'MIresult'
		
		# return it at the end of the function.
		rval
	}

# construct a way to subset sbo.svy objects,
# since they're actually two separate database-backed survey objects, not one.
subset.sbosvyimputationList <-
	function( x , ... ){
		
		# subset the survey object that's going to be used for
		# means, medians, totals, etc.
		coef.sub <- subset( x$coef , ... )
		
		# replicate `var.sub` so it's got all the same attributes as `x$var`
		var.sub <- x$var
		
		# but then overwrite the `designs` attribute with a subset
		var.sub$designs <- lapply( x$var$designs , subset , ... )
		
		# now re-create the `sbosvyimputationList` just as before
		sub.svy <-
			list(
				coef = coef.sub ,
				var = var.sub
			)
		
		# name it..
		class( sub.svy ) <- 'sbosvyimputationList'
		
		# ..class it..
		sub.svy$call <- sys.call(-1)
		
		# ..return it.  done.
		sub.svy
	}
