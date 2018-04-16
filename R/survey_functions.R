

#' variant of survey::svyttest that works on multiply-imputed data
#'
#' @seealso \url{http://r-survey.r-forge.r-project.org/survey/html/svyttest.html}
#'
#' @export
MIsvyttest<-function(formula, design , ...){

	# the MIcombine function runs differently than a normal svyglm() call
	m <- eval(bquote(mitools::MIcombine( with( design , survey::svyglm(formula,family=gaussian()))) ) )

	rval<-list(statistic=coef(m)[2]/survey::SE(m)[2],
			   parameter=m$df[2],
			   estimate=coef(m)[2],
			   null.value=0,
			   alternative="two.sided",
			   method="Design-based t-test",
			   data.name=deparse(formula))

	rval$p.value <- ( 1 - pf( ( rval$statistic )^2 , 1 , m$df[2] ) )

	names(rval$statistic)<-"t"
	names(rval$parameter)<-"df"
	names(rval$estimate)<-"difference in mean"
	names(rval$null.value)<-"difference in mean"
	class(rval)<-"htest"

	return(rval)

}

#' variant of survey::svyciprop that works on multiply-imputed data
#'
#' @seealso \url{http://r-survey.r-forge.r-project.org/survey/html/svyciprop.html}
#'
#' @export
MIsvyciprop <-
	function (formula, design, method = c("logit", "likelihood",
		"asin", "beta", "mean", "xlogit"), level = 0.95, df = mean(unlist(lapply(design$designs,survey::degf))),...)
	{
		method <- match.arg(method)
		if (method == "mean") {
			m <- eval(bquote(mitools::MIcombine(with(design,survey::svymean(~as.numeric(.(formula[[2]])),...)))))
			ci <- as.vector(confint(m, 1, level = level, df = df,
				...))
			rval <- coef(m)[1]
			attr(rval, "var") <- vcov(m)
		}
		else if (method == "asin") {
			m <- eval(bquote(mitools::MIcombine(with(design,survey::svymean(~as.numeric(.(formula[[2]])), ...)))))
			m <- structure(coef(m), .Names = "1", var = m$variance[1], .Dim = c(1L, 1L), statistic = "mean", class = "svystat")
			xform <- survey::svycontrast(m, quote(asin(sqrt(`1`))))
			ci <- sin(as.vector(confint(xform, 1, level = level,
				df = df, ...)))^2
			rval <- coef(m)[1]
			attr(rval, "var") <- vcov(m)
		}
		else if (method == "xlogit") {
			m <- eval(bquote(mitools::MIcombine(with(design,survey::svymean(~as.numeric(.(formula[[2]])), ...)))))
			m <- structure(coef(m), .Names = "1", var = m$variance[1], .Dim = c(1L, 1L), statistic = "mean", class = "svystat")
			xform <- survey::svycontrast(m, quote(log(`1`/(1 - `1`))))
			ci <- expit(as.vector(confint(xform, 1, level = level,
				df = df, ...)))
			rval <- coef(m)[1]
			attr(rval, "var") <- vcov(m)
		}
		else if (method == "beta") {
			m <- eval(bquote(mitools::MIcombine(with(design,survey::svymean(~as.numeric(.(formula[[2]])), ...)))))
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
			m <- eval(bquote(mitools::MIcombine(with(design,svyglm(.(formula[[2]]) ~ 1, family = quasibinomial)))))
			cimethod <- switch(method, logit = "Wald", likelihood = "likelihood")
			ci <- suppressMessages(as.numeric(expit(confint(m, 1,
				level = level, method = cimethod, ddf = df))))
			rval <- expit(coef(m))[1]
			attr(rval, "var") <- vcov(eval(bquote(mitools::MIcombine(with(design,survey::svymean(~as.numeric(.(formula[[2]])), ...))))))
		}
		halfalpha <- (1 - level)/2
		names(ci) <- paste(round(c(halfalpha, (1 - halfalpha)) *
			100, 1), "%", sep = "")
		names(rval) <- deparse(formula[[2]])
		attr(rval, "ci") <- ci
		class(rval) <- "svyciprop"
		rval
	}


# pulled from the R survey library
expit <- function(eta) exp(eta)/(1 + exp(eta))

# pulled from the R miceadds library
miceadds_micombine.chisquare <-
	function (dk, df, display = TRUE, version = 1) {
		M <- length(dk)
		if (version == 0) {
			mean.dk <- mean(dk)
			sdk.square <- stats::var(sqrt(dk))
			Dval <- (mean.dk/df - (1 - 1/M) * sdk.square)/(1 + (1 + 
				1/M) * sdk.square)
			df2 <- (M - 1)/df^(3/M) * (1 + M/(M + 1/M)/sdk.square)^2
		}
		if (version == 1) {
			g2 <- dk
			m <- length(g2)
			g <- sqrt(g2)
			mg2 <- sum(g2)/m
			r <- (1 + 1/m) * (sum(g^2) - (sum(g)^2)/m)/(m - 1)
			Dval <- (mg2/df - r * (m + 1)/(m - 1))/(1 + r)
			df2 <- (m - 1) * (1 + 1/r)^2/df^(3/m)
		}
		pval <- stats::pf(Dval, df1 = df, df2 = df2, lower.tail = FALSE)
		chisq.approx <- Dval * df
		p.approx <- 1 - stats::pchisq(chisq.approx, df = df)
		res <- c(D = Dval, p = pval, df = df, df2 = df2)
		if (display) {
			cat("Combination of Chi Square Statistics for Multiply Imputed Data\n")
			cat(paste("Using", M, "Imputed Data Sets\n"))
			cat(paste("F(", df, ", ", round(df2, 2), ")", "=", round(Dval, 
				3), "     p=", round(pval, 5), sep = ""), "\n")
		}
		invisible(res)
	}


#' variant of survey::svychisq that works on multiply-imputed data
#'
#' @seealso \url{http://r-survey.r-forge.r-project.org/survey/html/svychisq.html}
#'
#' @export
MIsvychisq<-function(formula, design , statistic = "Chisq" , ... ) {

  if ( !( statistic %in% c( "Chisq" ) ) ) { stop( " This method is only implemented for `statistic = 'Chisq'`." ) }

  m <- with( design , svychisq( formula , statistic = statistic ) )

  dk  <- as.numeric( sapply( m , FUN = function( x ) x[["statistic"]] ) )
  df <- as.numeric( sapply( m , FUN = function( x ) x[["parameter"]][ "df" ] ) )

  return( miceadds_micombine.chisquare( dk=dk, df=df[[1]] , display = TRUE , version = 1 ) )

}
