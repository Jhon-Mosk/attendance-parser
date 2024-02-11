require("dotenv").config();
const cheerio = require("cheerio");
const log = require("logger-for-yc-functions")(module);
const { queryBuilder } = require("./modules/queryBuilder");
const { initDbConnection, executeQuery } = require("./modules/ydb");

const saveToYdb = async (count) => {
  await initDbConnection();
  const tableName = "attendance";
  const date = new Date();
  const payload = {
    date: date.getTime(),
    year: date.getUTCFullYear(),
    month: date.getUTCMonth() + 1,
    day: date.getUTCDate(),
    hour: date.getUTCHours(),
    minute: date.getUTCMinutes(),
    count,
  };
  const query = queryBuilder().upsert(payload).into(tableName).build();

  const result = await executeQuery(query);
  log.debug(result, "createDataResult");

  if (result === 200) {
    return payload.date;
  }

  return result;
};

const parseOnlinePeopleContent = (jsonData) => {
  const target = jsonData?.SLIDER?.ALL_BLOCK;
  if (!target) return;
  const $ = cheerio.load(target);
  const onlinePeopleContent = $("div.online-people_rz").text();
  return parseInt(onlinePeopleContent.split(" ")[1]);
};

module.exports.handler = async function (event, context) {
  try {
    const res = await fetch(process.env.TARGET_API_URL, {
      method: "POST",
      body: process.env.TARGET_AUTH_BODY,
      headers: {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
      },
    });

    if (res.ok) {
      const data = await res.json();
      const visitors = parseOnlinePeopleContent(data);
      log.debug(visitors, "visitors");
      const ids = await saveToYdb(visitors);
      log.debug(ids, "ids");
    } else {
      log.error(res.status + " : " + res.statusText, "error");
    }
  } catch (error) {
    log.error(error, "error");
  }

  return {
    statusCode: 200,
    body: "OK",
  };
};
