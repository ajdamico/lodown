get_catalog_sbo <-
	function( data_name = "sbo" , output_dir , ... ){
		
		
	catalog <-
		data.frame(
			full_url = "https://www2.census.gov/programs-surveys/sbo/datasets/2007/pums_csv.zip" ,
			output_filename = paste0( output_dir , "/2007 main.rds" ) ,
			stringsAsFactors = FALSE
		)

	catalog

}


lodown_sbo <-
	function( data_name = "sbo" , catalog , ... ){
	
		on.exit( print( catalog ) )

		if ( !requireNamespace( "mitools" , quietly = TRUE ) ) stop( "mitools needed for this function to work. to install it, type `install.packages( 'mitools' )`" , call. = FALSE )

		if( nrow( catalog ) != 1 ) stop( "sbo catalog must be exactly one record" )
	
		tf <- tempfile()
	
		cachaca( catalog$full_url , tf , mode = 'wb' , filesize_fun = 'unzip_verify' )
		
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
		
		saveRDS( sbo_svy , file = catalog$output_filename , compress = FALSE ) ; rm( sbo_svy ) ; gc()
		
		# delete the temporary files
		suppressWarnings( file.remove( tf , unzipped_files ) )

		cat( paste0( data_name , " catalog entry " , 1 , " of " , nrow( catalog ) , " stored at '" , catalog$output_filename , "'\r\n\n" ) )

		on.exit()
		
		catalog

	}


# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # functions related to survey statistical analysis  # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # #


#' dual design calculations for the survey of business owners
#'
#' the \code{mitools::MIcombine} variant includes a 2007-specific variance adjustment.  this will change in other years.
#' this adjustment statistic was pulled from the middle of page 8
#' \url{https://www2.census.gov/econ/sbo/07/pums/2007_sbo_pums_users_guide.pdf#page=8}
#'
#' each of these sbo-specific functions contain a variant of some other \code{library(survey)} function that also maintains the census bureau's dual design calculation.
#' these functions expect both the coefficient and the variance survey objects
#'
#' @seealso \url{https://cran.r-project.org/web/packages/mitools/mitools.pdf}
#'
#' @rdname sbo
#' @export
sbo_MIcombine <-
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



#' @rdname sbo
#' @export
sbo_with <-
	function ( sbo.svy , expr , ... ){
	
		pf <- parent.frame()
		
		expr <- substitute( expr )
		
		expr$design <- as.name(".design")

		# this pulls in means, medians, totals, etc.
		# notice it uses sbo.svy$coef
		results <- eval( expr , list( .design = sbo.svy$coef ) )
		
		gc()
		
		# this is used to calculate the variance, adjusted variance, standard error
		# notice it uses the sbo.svy$var object
		variances <- 
			lapply( 
				sbo.svy$var$designs , 
				function( .design ){ 
					eval( expr , list( .design = .design ) , enclos = pf ) 
				} 
			)
		
		gc()
		
		# combine both results..
		rval <- list( coef = results , var = variances )
		
		# ..into a brand new object class
		class( rval ) <- 'imputationResultList'
		
		gc()
		
		# and return it.
		rval
	}



#' @rdname sbo
#' @export
sbo_subset <-
	function( x , ... ){
		
		# subset the survey object that's going to be used for
		# means, medians, totals, etc.
		coef.sub <- subset( x$coef , ... )
		
		gc()
		
		# replicate `var.sub` so it's got all the same attributes as `x$var`
		var.sub <- x$var
		
		# but then overwrite the `designs` attribute with a subset
		var.sub$designs <- lapply( x$var$designs , subset , ... )
		
		gc()
		
		# now re-create the `sbosvyimputationList` just as before
		sub.svy <-
			list(
				coef = coef.sub ,
				var = var.sub
			)
		
		# ..class it..
		sub.svy$call <- sys.call(-1)
		
		gc()
		
		# ..return it.  done.
		sub.svy
	}

	
#' @rdname sbo
#' @export
sbo_update <-
	function( x , ... ){
		
		# update the survey object that's going to be used for
		# means, medians, totals, etc.
		coef.upd <- update( x$coef , ... )
		
		gc()
		
		# replicate `var.upd` so it's got all the same attributes as `x$var`
		var.upd <- x$var
		
		# but then overwrite the `designs` attribute with an update
		var.upd$designs <- lapply( x$var$designs , update , ... )
		
		gc()
		
		# now re-create the `sbosvyimputationList` just as before
		upd.svy <-
			list(
				coef = coef.upd ,
				var = var.upd
			)
			
		gc()
		
		# ..return it.  done.
		upd.svy
	}

#' @rdname sbo
#' @export
sbo_degf <- function( x ) survey:::degf( x$coef )


#' @rdname sbo
#' @export
sbo_MIsvyciprop <-
	function (formula, design, method = c("logit", "likelihood",
		"asin", "beta", "mean", "xlogit"), level = 0.95, df = mean(unlist(lapply(design$designs,survey::degf))),
		...)
	{
		method <- match.arg(method)
		if (method == "mean") {
			m <- eval(bquote(sbo_MIcombine(sbo_with(design,survey::svymean(~as.numeric(.(formula[[2]])),...)))))
			ci <- as.vector(confint(m, 1, level = level, df = df,
				...))
			rval <- coef(m)[1]
			attr(rval, "var") <- vcov(m)
		}
		else if (method == "asin") {
			m <- eval(bquote(sbo_MIcombine(sbo_with(design,survey::svymean(~as.numeric(.(formula[[2]])), ...)))))
			m <- structure(coef(m), .Names = "1", var = m$variance[1], .Dim = c(1L, 1L), statistic = "mean", class = "svystat")
			xform <- survey::svycontrast(m, quote(asin(sqrt(`1`))))
			ci <- sin(as.vector(confint(xform, 1, level = level,
				df = df, ...)))^2
			rval <- coef(m)[1]
			attr(rval, "var") <- vcov(m)
		}
		else if (method == "xlogit") {
			m <- eval(bquote(sbo_MIcombine(sbo_with(design,survey::svymean(~as.numeric(.(formula[[2]])), ...)))))
			m <- structure(coef(m), .Names = "1", var = m$variance[1], .Dim = c(1L, 1L), statistic = "mean", class = "svystat")
			xform <- survey::svycontrast(m, quote(log(`1`/(1 - `1`))))
			ci <- expit(as.vector(confint(xform, 1, level = level,
				df = df, ...)))
			rval <- coef(m)[1]
			attr(rval, "var") <- vcov(m)
		}
		else if (method == "beta") {
			m <- eval(bquote(sbo_MIcombine(sbo_with(design,survey::svymean(~as.numeric(.(formula[[2]])), ...)))))
			n.eff <- coef(m) * (1 - coef(m))/vcov(m)
			rval <- coef(m)[1]
			attr(rval, "var") <- vcov(m)
			alpha <- 1 - level
			n.eff <- n.eff * (qt(alpha/2, mean(unlist(lapply(design$designs,nrow))) - 1)/qt(alpha/2,
				mean(unlist(lapply(design$designs,survey::degf)))))^2
			ci <- c(qbeta(alpha/2, n.eff * rval, n.eff * (1 - rval) +
				1), qbeta(1 - alpha/2, n.eff * rval + 1, n.eff *
				(1 - rval)))
		}
		else {
			m <- eval(bquote(sbo_MIcombine(sbo_with(design,svyglm(.(formula[[2]]) ~ 1, family = quasibinomial)))))
			cimethod <- switch(method, logit = "Wald", likelihood = "likelihood")
			ci <- suppressMessages(as.numeric(expit(confint(m, 1,
				level = level, method = cimethod, ddf = df))))
			rval <- expit(coef(m))[1]
			attr(rval, "var") <- vcov(eval(bquote(sbo_MIcombine(sbo_with(design,survey::svymean(~as.numeric(.(formula[[2]])), ...))))))
		}
		halfalpha <- (1 - level)/2
		names(ci) <- paste(round(c(halfalpha, (1 - halfalpha)) *
			100, 1), "%", sep = "")
		names(rval) <- deparse(formula[[2]])
		attr(rval, "ci") <- ci
		class(rval) <- "svyciprop"
		rval
	}

