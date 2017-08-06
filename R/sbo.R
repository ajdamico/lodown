get_catalog_sbo <-
	function( data_name = "sbo" , output_dir , ... ){
		
		
	catalog <-
		data.frame(
			full_url = "https://www2.census.gov/econ/sbo/07/pums/pums_csv.zip" ,
			output_filename = paste0( output_dir , "/2007 main.rds" ) ,
			stringsAsFactors = FALSE
		)

	catalog

}


lodown_sbo <-
	function( data_name = "sbo" , catalog , ... ){
	
		if ( !requireNamespace( "mitools" , quietly = TRUE ) ) stop( "mitools needed for this function to work. to install it, type `install.packages( 'mitools' )`" , call. = FALSE )

		if( nrow( catalog ) != 1 ) stop( "sbo catalog must be exactly one record" )
	
		tf <- tempfile()
	
		cachaca( catalog$full_url , tf , mode = 'wb' , filesize_fun = 'httr' )
		
		unzipped_files <- unzip_warn_fail( tf , exdir = tempdir() )
	
		x <- data.frame( readr::read_csv( unzipped_files ) ) ; gc()
		
		names( x ) <- tolower( names( x ) )
		
		x$one <- 1
		
		# and use the weights displayed in the census bureau's technical documentation
		x$newwgt <- 10 * x$tabwgt * sqrt( 1 - 1 / x$tabwgt )
		# https://www2.census.gov/econ/sbo/07/pums/2007_sbo_pums_users_guide.pdf#page=7

		var_list <- NULL
		
		for( i in 1:10 ) { var_list <- c( var_list , list( subset( x , rg == i ) ) ) ; gc() }
		
		#####################################################
		# survey design for a hybrid database-backed object #
		#####################################################

		# create a survey design object with the SBO design
		# to use for the coefficients: means, medians, totals, etc.
		sbo_coef <-
			survey::svydesign(
				id = ~1 ,
				weight = ~tabwgt ,
				data = x
			)
		rm( x ) ; gc()
		# this one just uses the original table `x`

		# create a survey design object with the SBO design
		# to use for the variance and standard error
		sbo_var <-
			survey::svydesign(
				id = ~1 ,
				weight = ~newwgt ,
				data = mitools::imputationList( var_list )
			)
		
		rm( var_list ) ; gc()
		# this one uses the ten `x1` thru `x10` tables you just made.


		# slap 'em together into a single list object..
		sbo_svy <- list( coef = sbo_coef , var = sbo_var )
		class( sbo_svy ) <- 'sbosvyimputationList'
		
		saveRDS( sbo_svy , file = catalog$output_filename ) ; rm( sbo_svy ) ; gc()
		
		# delete the temporary files
		suppressWarnings( file.remove( tf , unzipped_files ) )

		cat( paste0( data_name , " catalog entry " , 1 , " of " , nrow( catalog ) , " stored at '" , catalog$output_filename , "'\r\n\n" ) )

		catalog

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
# https://www2.census.gov/econ/sbo/07/pums/2007_sbo_pums_users_guide.pdf#page=8
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
