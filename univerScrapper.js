import * as credentials from './credentials.js';
import axios from "axios"
import mysql from 'mysql';
import fetch from "node-fetch";

let con = mysql.createConnection({
    host: credentials.dbHost,
    user: credentials.dbUser,
    password: credentials.dbPassword,
    database: credentials.database
});

const salesNavSearchExportId = credentials.salesNavSearchExport;
const phantomBusterApiKey = credentials.phantomBusterApiKey;
// Credentials for phantombuster
const initOptions = {
    headers: {
        "x-phantombuster-key": phantomBusterApiKey,
        "Content-Type": "application/json",
    },
}

async function buildSearchUrl(functionId, industryId, geographyId, salesNavSearchSession) {
    let geography = await getGeographyById(geographyId);
    let industry = await getIndustryById(industryId);
    let functionObj = await getFunctionById(functionId);
    return 'https://www.linkedin.com/sales/search/people?doFetchHeroCard=false' +
        '&functionIncluded=' + functionObj.linkedin_id +
        '&geoIncluded=' + geography.linkedin_id +
        '&industryIncluded=' + industry.linkedin_id +
        '&logHistory=true' +
        '&rsLogId=1099039369' +
        '&schoolIncluded=18633' + // CHANGE TO PARSE OTHER SCHOOLS
        '&searchSessionId=' + salesNavSearchSession
}

// Check phantombuster search process
async function checkStatus(agentId) {
    let url = 'https://api.phantombuster.com/api/v2/agents/fetch-output?id=' + agentId;
    let result = '';
    let options = {
        method: 'GET',
        headers: {
            Accept: 'application/json',
            'X-Phantombuster-Key': phantomBusterApiKey
        }
    };

    let response = await fetch(url, options)
    if (response.ok) {
        result = await response.json();
        return result.status;
    }
}

// Get and save results of searches
async function getResults(containerId) {
    let url = 'https://api.phantombuster.com/api/v1/agent/' + salesNavSearchExportId + '/output';
    console.log('Container ID: ' + containerId)
    let options = {
        method: 'GET',
        qs: {containerId: containerId, withoutResultObject: 'false'},
        headers: {
            Accept: 'application/json',
            'X-Phantombuster-Key': phantomBusterApiKey
        }
    };
    let status = '';
    let result = '';
    // Receiving status, if finished than go on.
    do {
        status = await checkStatus(salesNavSearchExportId);
        console.log(status)
    } while (status !== 'finished')
    let response = await fetch(url, options)
    if (response.ok) {
        result = await response.json();
        console.log(result)
        if (result.data.resultObject && result.data.resultObject.includes("No activity")) {
            return {
                notice: "No activity"
            }
        } else if (result.data.output.split('Error:')[1]) {
            return {
                error: result.data.output.split('Error:')[1]
            }
        } else if (result.data.output.includes("Can't connect to LinkedIn with this session cookie.")) {
            return {
                error: "Can't connect to LinkedIn with this session cookie."
            }
        } else if (result.data.resultObject) {
            return await JSON.parse(result.data.resultObject)
        } else {
            return false;
        }
    }
}

async function runSearchParser(query, sessionCookie) {
    return await new Promise((resolve) => {
        axios.post(
            "https://api.phantombuster.com/api/v2/agents/launch",
            {
                "id": salesNavSearchExportId,
                "argument":
                    {
                        "numberOfProfiles": 250,
                        "extractDefaultUrl": true,
                        "removeDuplicateProfiles": true,
                        "accountSearch": false,
                        "sessionCookie": sessionCookie,
                        "searches": query,
                        "numberOfResultsPerSearch": 2500,
                        "csvName": credentials.result_file
                    },
            },
            initOptions,
        )
            .then((res) => resolve(res.data.containerId))
            .catch((error) => console.error("Something went wrong :(", error))
    });
}

async function getSearchQueryByAccountId(accountId) {
    let sql = (`SELECT *
                FROM search_queries
                WHERE account_id = ${accountId} AND is_scraped = 0 LIMIT 1`);
    return await new Promise((resolve) => {
        con.query(sql, async function (err, result) {
            if (err) {
                throw err;
            }
            if (result.length >= 1) {
                resolve(result[0])
            } else {
                resolve(false);
            }
        });
    });
}

async function getLastSearchQuery() {
    let sql = (`SELECT *
                FROM search_queries
                ORDER BY id DESC
                LIMIT 1`);
    return await new Promise((resolve) => {
        con.query(sql, async function (err, result) {
            if (err) {
                throw err;
            }
            if (result.length >= 1) {
                resolve(result[0])
            } else {
                resolve(false);
            }
        });
    });
}

async function getAccounts() {
    let sql = (`SELECT *
                FROM accounts
                WHERE active = 1`);
    return await new Promise((resolve) => {
        con.query(sql, async function (err, result) {
            if (err) {
                throw err;
            }
            if (result.length >= 1) {
                resolve(result)
            } else {
                resolve(false);
            }
        });
    });
}

async function getIndustryById(id) {
    let sql = (`SELECT *
                FROM industries
                WHERE id = ${id} LIMIT 1`);
    return await new Promise((resolve) => {
        con.query(sql, async function (err, result) {
            if (err) {
                throw err;
            }
            if (result.length >= 1) {
                resolve(result[0])
            } else {
                resolve(false);
            }
        });
    });
}

async function getGeographyById(id) {
    let sql = (`SELECT *
                FROM geography
                WHERE id = ${id} LIMIT 1`);
    return await new Promise((resolve) => {
        con.query(sql, async function (err, result) {
            if (err) {
                throw err;
            }
            if (result.length >= 1) {
                resolve(result[0])
            } else {
                resolve(false);
            }
        });
    });
}

async function getFunctionById(id) {
    let sql = (`SELECT *
                FROM functions
                WHERE id = ${id} LIMIT 1`);
    return await new Promise((resolve) => {
        con.query(sql, async function (err, result) {
            if (err) {
                throw err;
            }
            if (result.length >= 1) {
                resolve(result[0])
            } else {
                resolve(false);
            }
        });
    });
}

async function createIndustry(name, linkedInId) {
    let sql = (`INSERT INTO industries (name, linkedin_id) VALUES ('${name}' ,  '${linkedInId}' ) `);
    return await new Promise((resolve) => {
        con.query(sql, function (err, result) {
            if (err) {
                throw err;
            } else {
                console.log("Successfully saved!")
            }
            resolve(result.insertId);
        });
    });
}

async function createFunction(name, linkedInId) {
    let sql = (`INSERT INTO functions (name, linkedin_id) VALUES ('${name}' ,  '${linkedInId}' ) `);
    return await new Promise((resolve) => {
        con.query(sql, function (err, result) {
            if (err) {
                throw err;
            } else {
                console.log("Successfully saved!")
            }
            resolve(result.insertId);
        });
    });
}

async function createNewQuery(query) {
    let sql = (`INSERT INTO search_queries (geography_id, function_id, industry_id, search_url, account_id, is_scraped) VALUES (${query.geography_id} , ${query.function_id} , ${query.industry_id} ,  '${query.search_url}', ${query.account_id}, ${query.is_scraped} ) `);
    return await new Promise((resolve) => {
        con.query(sql, function (err, result) {
            if (err) {
                throw err;
            } else {
                console.log("Successfully saved!")
            }
            resolve(result.insertId);
        });
    });
}

async function createAlumni(result, industryId, functionId) {
    let sql = (`INSERT INTO alumnis (name, last_name, full_name, linkedin_url, industry_id, title, function_id, location, duration, past_role, past_company, past_company_url, sales_nav_url)
                    VALUES ("${result.firstName}", "${result.lastName}", "${result.fullName}", "${result.defaultProfileUrl}", ${industryId}, "${result.title}", ${functionId}, "${result.location}", "${result.duration}","${result.pastRole}","${result.pastCompany}","${result.pastCompanyUrl}","${result.profileUrl}" ) `);
    return await new Promise((resolve) => {
        con.query(sql, function (err, result) {
            if (err) {
                throw err;
            } else {
                console.log("Alumni saved!")
                resolve(result.insertId);
            }
        });
    });
}

async function updateSearchQuery(searchQueryId) {
    let sql = (`UPDATE search_queries SET is_scraped = 1 WHERE id = "${searchQueryId}"`);
    return await new Promise((resolve) => {
        con.query(sql, async function (err, result) {
            if (err) {
                throw err;
            }
            resolve(result);
        });
    });
}

async function processNewQuery(account) {
    let query = {};
    let lastQuery = await getLastSearchQuery();
    if (lastQuery === false) {
        query = {
            geography_id: 1,
            function_id: 1,
            industry_id: 1,
            search_url: '',
            account_id: account.id,
            is_scraped: 0
        };
    } else {
        let newQuery = {
            geography_id: 0,
            function_id: 0,
            industry_id: 0,
            search_url: '',
            account_id: account.id,
            is_scraped: 0
        }

        let newIndustry = await getIndustryById(lastQuery.industry_id + 1);
        if (newIndustry === false) {
            let newFunction = await getFunctionById(lastQuery.function_id + 1);
            if (newFunction === false) {
                console.log("FAILED TO RECEIVE NEW FUNCTION, CHECK IF ALL PARSED OR ERROR OCCURED!");
                process.exit(1);
            }
            newQuery.function_id = newFunction.id;
            newQuery.industry_id = 1;
            newQuery.geography_id = lastQuery.geography_id;
            query = newQuery;
        } else {
            newQuery.function_id = lastQuery.function_id;
            newQuery.industry_id = newIndustry.id;
            newQuery.geography_id = lastQuery.geography_id;
            query = newQuery;
        }
    }
    try {
        query.search_url = await buildSearchUrl(query.function_id, query.industry_id, query.geography_id, account.sales_nav_search_session_id);
        await createNewQuery(query);
        return query;
    } catch (e) {
        console.log("Failed to save data to database, please check data and MYSQL connection :")
        console.log(e);
        process.exit(1);
    }
}

async function startSearch() {
//module.exports.startSearch = async function (accountId) {
    if (await getIndustryById(1) === false) {
        let industriesArray = credentials.industries.elements;
        for (let industry of industriesArray) {
            await createIndustry(industry.displayValue, industry.id);
        }
    }
    if (await getFunctionById(1) === false) {
        let functionsArray = credentials.functionsData.elements;
        for (let functionObj of functionsArray) {
            await createFunction(functionObj.displayValue, functionObj.id);
        }
    }
    let accountsArray = await getAccounts();
    for (let account of accountsArray) {
        let results = false;
        do {
            let searchQuery = await getSearchQueryByAccountId(account.id);
            if (searchQuery === false) {
                searchQuery = await processNewQuery(account);
            }
            console.log(searchQuery);
            let containerId = await runSearchParser(searchQuery.search_url, account.session_token);
            results = await getResults(containerId, salesNavSearchExportId);
            if (results.error) {
                console.log(results.error);
                continue;
            }
            console.log(results);
            if (results !== false) {
                for (let alumni of results) {
                    if (!alumni.error) {
                        await createAlumni(alumni, searchQuery.industry_id, searchQuery.function_id);
                    }
                }
            } else {
                await updateSearchQuery(searchQuery.id);
            }
        } while (results === false)
    }
    con.end();
}

startSearch();