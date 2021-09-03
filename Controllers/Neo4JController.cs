using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Neo4j.Driver;
using System.Text;
using System.Text.RegularExpressions;

namespace graphAPI.Controllers
{
    [ApiController]
    [Route("api")]
    public class Neo4Jcontroller : ControllerBase
    {
        private readonly IDriver _driver;
        private readonly ILogger<Neo4Jcontroller> _logger;

        public Neo4Jcontroller(ILogger<Neo4Jcontroller> logger)
        {
            _driver = GraphDatabase.Driver("bolt://neo4j:7687", AuthTokens.Basic("neo4j", "neo4j"));
            _logger = logger;
        }

        /// <summary>
        /// POST endpoint for creation of a new node in the database
        /// </summary>
        /// <param name="name">
        /// String with the name of a new asset to be created
        /// </param>
        /// <param name="price">
        /// String value of the purchase price of the asset
        /// Has to be properly parse to float
        /// </param>
        /// <param name="date">
        /// String date of the purchase of an asset
        /// Must come in a dd/MM/yyyy format
        /// </param>
        /// <returns>
        /// Status code 201 and the created asset if successful
        /// Status code 403 and error message if any of the parameters are empty
        /// Status code 403 and error message if any of the parametres do not conform to the requirements</returns>
        [HttpPost("asset/create")]
        public async Task<IActionResult> CreateNode(string name, string price, string date)
        {
            // variable to store the parsed inputs
            decimal parsedPrice = new();
            DateTime parsedDate = new();

            //
            // D A T A   V A L I D A T I O N
            //
            try
            {
                // validate the name value
                _ = ValidateString(name, nameof(name));

                // try getting an exact parse of the float string
                parsedPrice = ParsePrice(ValidateString(price, nameof(price)));

                // try getting an exact parse of the data string
                parsedDate = ParseDate(ValidateString(date, nameof(date)));

            }
            catch(Exception exc) when (exc is ArgumentNullException || exc is FormatException)
            {
                // if validation found a problem, communicate it to the user
                return StatusCode(403, exc.Message);
            }


            //
            // Q U E R Y   C O N S T R U C T I O N
            //
            // string builder to create the query text
            StringBuilder queryText = new();

            // create a new, unique id for the node
            // use of guid significantly lowers the chance of repeated IDs,
            // but the database should have a UNIQUE constraint on this field regardless
            string newId = Guid.NewGuid().ToString();

            // append the text to the builder
            _ = queryText.Append("CREATE (n:Asset {id: $newId, name: $name, price: $price, purchaseDate: $date}) RETURN n");

            // dictionary of parametres 
            Dictionary<string, object> queryParameters = new()
            {
                { "name", name },
                { "newId", newId },
                { "price", parsedPrice },
                { "date", parsedDate }
            };

            //
            // Q U E R Y   P R O C E S S I N G
            //
            // list for the results of the query
            List<object> result = new();

            // open an async session
            var session = _driver.AsyncSession();

            try
            {
                // run async reading transaction
                result = await session.WriteTransactionAsync(async tx =>
                {
                    // inner list for resulting objects
                    List<object> matches = new();

                    // cursor on the results of the async query
                    var writer = await tx.RunAsync(queryText.ToString(), queryParameters);

                    // loop while waiting for the end of fetching the results
                    while (await writer.FetchAsync())
                    {
                        // save a result in the inner list
                        matches.Add(writer.Current[0]);
                    }

                    // return inner list
                    return matches;
                });
            }
            finally
            {
                // when all is done, close the connection
                await session.CloseAsync();
            }

            // return the search results
            return StatusCode(201, result);
        }

        /// <summary>
        /// POST endpoint for a multi-parametre search of the database
        /// Accepts any combination of parametres
        /// Empty parametres are treated as not provided
        /// </summary>
        /// <param name="id">
        /// String Id to perform an *exact* match on
        /// </param>
        /// <param name="name">
        /// String Name to perform a *partial* match on
        /// </param>
        /// <param name="priceEqual">
        /// Decimal price to perform an *exact* match on
        /// </param>
        /// <param name="priceGreaterThan">
        /// Decimal price to perform a *greater than* match on
        /// </param>
        /// <param name="priceLesserThan">
        /// Decimal price to perform a *lesser than* match on
        /// </param>
        /// <param name="dateOn">
        /// DateTime (dd/MM/yyyy HH:mm) purchase date to perform an *exact* match on
        /// </param>
        /// <param name="dateAfter">
        /// DateTime (dd/MM/yyyy HH:mm) purchase date to perform a *greater than* match on
        /// </param>
        /// <param name="dateBefore">
        /// DateTime (dd/MM/yyyy HH:mm) purchase date to perform a *lesser than* match on
        /// </param>
        /// <param name="whichLinks">
        /// A specifier for whether you return assets linked to the matching assets through inbound, outbound, or both types of links
        /// </param>
        /// <returns>
        /// Status code 200 and an array of nodes matching the criteria on success;
        /// if the request specified returning linked nodes, the result will be an array of arrays (database rows) containing found asset and linked assets
        /// Status code 403 and error message if any Decimal or DateTime parameter is not valid
        /// Status code 403 and error message if no parameters were passed
        /// </returns>
        [HttpPost("asset/find")]
        public async Task<IActionResult> FindNode(string id, string name, string priceEqual, string priceGreaterThan, string priceLesserThan, string dateOn, string dateAfter, string dateBefore, string whichLinks)
        {
            //
            // Q U E R Y   C O N S T R U C T I O N
            //
            // string builder to create the query text
            StringBuilder queryText = new();

            // add the base of the query
            _ = queryText.Append("MATCH (n:Asset) WHERE ");

            // dictionary of query elements associated with each argument
            Dictionary<string, string> argsToQueryDict = new()
            {
                { nameof(id), "n.id = $id" },
                { nameof(name), "ToLower(n.name) CONTAINS ToLower($name)" },
                { nameof(priceEqual), "n.price = $priceEqual" },
                { nameof(priceGreaterThan), "n.price > $priceGreaterThan" },
                { nameof(priceLesserThan), "n.price < $priceLesserThan" },
                { nameof(dateOn), "n.purchaseDate = $dateOn" },
                { nameof(dateAfter), "n.purchaseDate > $dateAfter" },
                { nameof(dateBefore), "n.purchaseDate < $dateBefore" }
            };

            // dictionary of param values
            Dictionary<string, string> paramValues = new()
            {
                { nameof(id), id },
                { nameof(name), name },
                { nameof(priceEqual), priceEqual },
                { nameof(priceGreaterThan), priceGreaterThan },
                { nameof(priceLesserThan), priceLesserThan },
                { nameof(dateOn), dateOn },
                { nameof(dateAfter), dateAfter },
                { nameof(dateBefore), dateBefore }
            };

            // dictionary of query params
            Dictionary<string, object> queryParameters = new();

            // list to store criteria to construct the query from
            List<string> argsToQuery = new();

            // for each argument
            foreach(var item in paramValues)
            {
                // if it has a value
                // do NOT use ValidateString here -- arguments in this endpoint
                // can explicitly be empty, therefore the check is just to ensure correct
                // query building
                if (item.Value != "" && item.Value is not null)
                {
                    // store its search criterion in a list
                    argsToQuery.Add(argsToQueryDict[item.Key]);

                    // store the parsed value
                    // parse prices as decimals
                    if (item.Key.Contains("price"))
                    {
                        queryParameters.Add(item.Key, ParsePrice(item.Value));
                    }
                    // parse dates as DateTime
                    else if (item.Key.Contains("date"))
                    {
                        queryParameters.Add(item.Key, ParseDate(item.Value));
                    }
                    // otherwise use raw string
                    else
                    {
                        queryParameters.Add(item.Key, item.Value);
                    }
                    
                }
            }

            // if the list of query bits is empty, no params were passed
            // terminate the operation and return appropriate status code
            if (argsToQuery.Count <= 0)
            {
                return StatusCode(403, "You have to provide at least one search parameter.");
            }

            // extend the search query with saved criteria
            _ = queryText.Append(string.Join(" AND ", argsToQuery));

            // append optional match for neighbours based on the parameter
            if (whichLinks == "in" || whichLinks == "both")
            {
                _ = queryText.Append(" OPTIONAL MATCH (n)<--(inLinks)");
            }
            if (whichLinks == "out" || whichLinks == "both")
            {
                _ = queryText.Append(" OPTIONAL MATCH (n)-->(outLinks)");
            }

            // and close with a return statement
            _ = queryText.Append(" RETURN [n");

            // if links are expected, add return statements
            if (whichLinks == "in" || whichLinks == "both")
            {
                _ = queryText.Append(", inLinks");
            }
            if (whichLinks == "out" || whichLinks == "both")
            {
                _ = queryText.Append(", outLinks");
            }
            // close the return array
            _ = queryText.Append("]");

            //
            // Q U E R Y   P R O C E S S I N G
            //
            // list for the results of the query
            List<object> result = new();
            
            // open an async session
            var session = _driver.AsyncSession();

            try
            {
                // run async reading transaction
                result = await session.ReadTransactionAsync(async tx =>
                {
                    // inner list for resulting objects
                    List<object> matches = new();

                    // cursor on the results of the async query
                    var reader = await tx.RunAsync(queryText.ToString(), queryParameters);

                    // loop while waiting for the end of fetching the results
                    while (await reader.FetchAsync())
                    {
                        // save a result in the inner list
                        matches.Add(reader.Current[0]);
                    }

                    // return inner list
                    return matches;
                });
            }
            finally
            {
                // when all is done, close the connection
                await session.CloseAsync();
            }

            // return the search results
            return StatusCode(200, result);
        }

        /// <summary>
        /// POST endpoint for creating a relationship/link between two Assets
        /// </summary>
        /// <param name="idFrom">
        /// String id of the node the connection is drawn *from*
        /// </param>
        /// <param name="idTo">
        /// String id of the node the connection is drawn *to*
        /// </param>
        /// <param name="linkType">
        /// String name of the newly created relationship
        /// Can only contain alphanumeric characters and character _
        /// By default, length is limited to 20 characters
        /// </param>
        /// <returns>
        /// 201 code and an array [nodeFrom, nodeTo, link] upon success
        /// 200 code and status message if request was valid but did not result in link creation
        /// 403 code and an appropriate error message upon any param failing validation
        /// </returns>
        [HttpPost("link/create")]
        public async Task<IActionResult> CreateLink(string idFrom, string idTo, string linkType)
        {
            //
            // P A R A M   V A L I D A T I O N
            //
            try
            {
                _ = ValidateString(idFrom, nameof(idFrom));
                _ = ValidateString(idTo, nameof(idTo));
                _ = ValidateString(linkType, nameof(linkType));
            }
            catch(Exception exc) when (exc is ArgumentNullException)
            {
                return StatusCode(403, exc.Message);
            }

            //
            // Q U E R Y   C O N S T R U C T I O N
            //
            // string builder to create the query text
            StringBuilder queryText = new();

            // append the text to the builder
            _ = queryText.Append("MATCH (a:Asset), (b:Asset) WHERE a.id = $idFrom AND b.id = $idTo CREATE (a)-[r:");

            // WARNING
            // MAJOR VULNERABILITY
            //
            // String concatenation ahead. This is necessary as Neo4J does not allow parametrised relationship types
            // Any changes to ValidateLinkType method should be made with extreme caution
            // this section of the code should be rewritten once Neo4J driver allows a safer way of performing this operation
            //
            try
            {
                // append uppercase copy of validated linkType param
                _ = queryText.Append(ValidateLinkType(linkType, nameof(linkType)).ToUpper());
            }
            catch (Exception exc) when (exc is ArgumentNullException || exc is ArgumentException)
            {
                return StatusCode(403, exc.Message);
            }

            // close the query off
            _ = queryText.Append("]->(b) RETURN [a, b, r]");

            // dictionary of parametres 
            Dictionary<string, object> queryParameters = new()
            {
                { "idFrom", idFrom },
                { "idTo", idTo }
            };

            //
            // Q U E R Y   P R O C E S S I N G
            //
            // list for the results of the query
            List<object> result = new();

            // open an async session
            var session = _driver.AsyncSession();

            try
            {
                // run async reading transaction
                result = await session.WriteTransactionAsync(async tx =>
                {
                    // inner list for resulting objects
                    List<object> matches = new();

                    // cursor on the results of the async query
                    var writer = await tx.RunAsync(queryText.ToString(), queryParameters);

                    // loop while waiting for the end of fetching the results
                    while (await writer.FetchAsync())
                    {
                        // save a result in the inner list
                        matches.Add(writer.Current[0]);
                    }

                    // return inner list
                    return matches;
                });
            }
            finally
            {
                // when all is done, close the connection
                await session.CloseAsync();
            }

            // if resulting array empty, at least one node was not found
            // inform the client
            if (result.Count <= 0)
            {
                return StatusCode(200, "One or both nodes not found. Link has not been created.");
            }
            // otherwise, confirm creation and return the result
            else
            {
                return StatusCode(201, result);
            }
        }

        /// <summary>
        /// POST endpoint for deleting a relationship/link between two Assets
        /// </summary>
        /// <param name="idFrom">
        /// String id of the node the connection is drawn *from*
        /// </param>
        /// <param name="idTo">
        /// String id of the node the connection is drawn *to*
        /// </param>
        /// <param name="linkType">
        /// String name of the relationship to delete
        /// Can only contain alphanumeric characters and character _
        /// By default, length is limited to 20 characters
        /// </param>
        /// <returns>
        /// 201 code and an array [nodeFrom, nodeTo, link] upon success
        /// 200 code and status message if request was valid but did not result in link deletion
        /// 403 code and an appropriate error message upon any param failing validation
        /// </returns>
        [HttpPost("link/delete")]
        public async Task<IActionResult> DeleteLink(string idFrom, string idTo, string linkType)
        {
            //
            // P A R A M   V A L I D A T I O N
            //
            try
            {
                _ = ValidateString(idFrom, nameof(idFrom));
                _ = ValidateString(idTo, nameof(idTo));
                _ = ValidateString(linkType, nameof(linkType));
            }
            catch (Exception exc) when (exc is ArgumentNullException)
            {
                return StatusCode(403, exc.Message);
            }

            //
            // Q U E R Y   C O N S T R U C T I O N
            //
            // string builder to create the query text
            StringBuilder queryText = new();

            // append the text to the builder
            _ = queryText.Append("MATCH (a:Asset {id: $idFrom})-[r:");

            // WARNING
            // MAJOR VULNERABILITY
            //
            // String concatenation ahead. This is necessary as Neo4J does not allow parametrised relationship types
            // Any changes to ValidateLinkType method should be made with extreme caution
            // this section of the code should be rewritten once Neo4J driver allows a safer way of performing this operation
            //
            try
            {
                // append uppercase copy of validated linkType param
                _ = queryText.Append(ValidateLinkType(linkType, nameof(linkType)).ToUpper());
            }
            catch (Exception exc) when (exc is ArgumentNullException || exc is ArgumentException)
            {
                return StatusCode(403, exc.Message);
            }

            // close the query off
            _ = queryText.Append("]-(b:Asset {id: $idTo}) DELETE r RETURN [a, b, r]");

            // dictionary of parametres 
            Dictionary<string, object> queryParameters = new()
            {
                { "idFrom", idFrom },
                { "idTo", idTo }
            };

            //
            // Q U E R Y   P R O C E S S I N G
            //
            // list for the results of the query
            List<object> result = new();

            // open an async session
            var session = _driver.AsyncSession();

            try
            {
                // run async reading transaction
                result = await session.WriteTransactionAsync(async tx =>
                {
                    // inner list for resulting objects
                    List<object> matches = new();

                    // cursor on the results of the async query
                    var writer = await tx.RunAsync(queryText.ToString(), queryParameters);

                    // loop while waiting for the end of fetching the results
                    while (await writer.FetchAsync())
                    {
                        // save a result in the inner list
                        matches.Add(writer.Current[0]);
                    }

                    // return inner list
                    return matches;
                });
            }
            finally
            {
                // when all is done, close the connection
                await session.CloseAsync();
            }

            // if result is empty, then the specific link could not be found
            // inform the client
            if (result.Count <= 0)
            {
                return StatusCode(200, "There was no such link to delete");
            }
            // otherwise, return both nodes and the deleted link
            else
            {
                return StatusCode(201, result);
            }
        }

        /// <summary>
        /// Helper function to validate the date parameter is valid
        /// </summary>
        /// <param name="date">
        /// String value of the date field of an HTTP request
        /// </param>
        /// <returns>
        /// DateTime value if successfully parsed
        /// Throws ArgumentNullException on empty header
        /// Throws FormatException if Date in wrong format
        /// </returns>
        private static DateTime ParseDate(string date)
        {
            DateTime parsedDate;

            try
            {
                // validate the date parameter and return an error code if necessary
                parsedDate = DateTime.ParseExact(date, "dd/MM/yyyy HH:mm", System.Globalization.CultureInfo.InvariantCulture);
            }
            catch (FormatException)
            {
                throw new FormatException("The date has to follow the format of dd/MM/yyyy HH:mm.");
            }

            // return parsed date if everything is fine
            return parsedDate;
        }

        /// <summary>
        /// Helper function to validate if the price parameter is valid
        /// </summary>
        /// <param name="price">
        /// The string price provided in the original JSON header
        /// </param>
        /// <returns>
        /// Float parsed from the price header
        /// Throws ArgumentNullException on empty header
        /// Throws FormatException on string that does not parse to legal float
        /// </returns>
        private static decimal ParsePrice(string price)
        {
            decimal parsedPrice = new();

            if (!decimal.TryParse(price, out parsedPrice))
            {
                throw new System.FormatException("The price has to be provided in a number form, e.g. 12.47.");
            }

            return parsedPrice;
        }

        /// <summary>
        /// A method to validate a string for the purpose of acting as a relationship type
        /// Allows through only non-empty strings of no more than charLimit characters
        /// Only alphanumeric characters and a special character _ are allowed
        /// 
        /// WARNING: This method exists to aid in a workaround to deficiency of Neo4J Cyphers
        /// It should be immediately deprecated once it is possible to parametrize relationship types
        /// 
        /// </summary>
        /// <param name="value">
        /// String value intended as a type of relationship in the database
        /// </param>
        /// <param name="headerName">
        /// Name of the header field in the object sent along with the request
        /// </param>
        /// <param name="charLimit">
        /// Int value limiting the length of relationship type's name
        /// </param>
        /// <returns>
        /// Returns unmodified value if it is valid
        /// Otherwise raises ArgumentNullException on empty value
        /// Otherwise raises ArgumentException on too long value, or value containing illegal characters
        /// </returns>
        private static string ValidateLinkType(string value, string headerName, int charLimit = 20)
        {
            // validate that the name is not empty
            if (value == "" || value is null)
            {
                throw new System.ArgumentNullException(headerName, "The request header cannot be empty.");
            }

            // ensure the string does not exceed length limit
            if (value.Length >= charLimit)
            {
                throw new ArgumentException("The provided header value is too long.", headerName);
            }

            // create a sanitisation regex
            Regex rx = new("[a-zA-Z0-9_]");

            if (!rx.IsMatch(value))
            {
                throw new ArgumentException("The provided header value contains illegal characters (only numbers, letters and _ are allowed).", headerName);
            }

            return value;
        }

        /// <summary>
        /// Helper function to validate if a string value of header is not empty
        /// Function created for more readable code for the api/asset/create endpoint
        /// </summary>
        /// <param name="value">
        /// The value of a header of the request
        /// </param>
        /// <param name="headerName">
        /// The name of a header that has the value checked
        /// This is so that the exception can inform which header value was empty
        /// </param>
        /// <returns>
        /// The value of a header of the request
        /// Throws ArgumentNullEsception on empty header
        /// </returns>
        private static string ValidateString(string value, string headerName) 
        {
            // validate that the name is not empty
            if (value == "" || value is null)
            {
                throw new System.ArgumentNullException(headerName, "This request header cannot be empty.");
            }

            return value;
        }
    }
}
