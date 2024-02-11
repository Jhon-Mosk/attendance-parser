const {
  Driver,
  MetadataAuthService,
  TypedData,
  ExecuteQuerySettings,
  withRetries,
  RetryParameters,
  getLogger,
  TokenAuthService,
} = require("ydb-sdk");
const logger = getLogger();

let driver;

/**
 * инициализирует соединение с БД
 */
exports.initDbConnection = async function () {
  logger.info("Driver initializing...");
  if (!driver) {
    try {
      const authService =
        process.env.NODE_ENV === "local"
          ? new TokenAuthService(process.env.YC_IAM_TOKEN)
          : new MetadataAuthService();
      driver = new Driver({
        endpoint: process.env.YC_ENDPOINT,
        database: process.env.YC_DATABASE,
        authService,
      });

      const timeout = 10000;
      if (!(await driver.ready(timeout))) {
        logger.error(`Driver has not become ready in ${timeout}ms!`);
        process.exit(1);
      }
    } catch (e) {
      logger.error(`Driver initializing ends with error: ${e}`);
    }
  }
};

/**
 * разрывает соединение с БД
 */
exports.destroyDbConnection = async function () {
  await driver.destroy();
};

// параметры запроса - кешировать
const querySettings = new ExecuteQuerySettings().withKeepInCache(true);
const retrySettings = new RetryParameters({
  maxRetries: 2, // количество повторных запросов в случае ошибки
});

/**
 * запрос к ЯБД
 * @param {string} query - подготовленная или обычная строка запроса
 * @param {object} [queryParams] - параметры для подготовленной строки запроса
 * @returns {Promise<[{}]|number>} - результат запроса, при успехе [{}], если данных нет [], при ошибке 500
 */
exports.executeQuery = async (query, queryParams = null) => {
  try {
    // повторять запрос в случае ошибки
    return withRetries(async () => {
      return await driver.tableClient.withSession(async (session) => {
        if (queryParams) {
          logger.info("Execute query, preparing query...");
          query = await session.prepareQuery(query);
          logger.info("Query has been prepared, executing...");
        }

        const { resultSets } = await session.executeQuery(
          query,
          queryParams || {},
          { commitTx: true, beginTx: { serializableReadWrite: {} } },
          querySettings
        );

        if (resultSets.length !== 0) {
          return TypedData.createNativeObjects(resultSets[0]);
        }

        return 200;
      });
    }, retrySettings);
  } catch (error) {
    console.error("executeQueryError :>> " + error);
    console.error("errorInQuery :>> ", query);
    return 500;
  }
};
