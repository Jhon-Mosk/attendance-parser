/**
 * для построение строк запросов
 * @returns {Function} функции select - для запроса данных из таблицы, upsert - для сохранения или обновления одной строки, bulkUpsert - для множественного сохранения или обновления строк, максимальный размер данных 2,5 Мб???
 */
const queryBuilder = () => {
  /**
   * для запроса данных из таблицы
   * @param {Array} [fields='*'] - с названия требуемых столбцов
   * @returns {Function} с from
   */
  const select = (fields) => {
    const params = {};

    fields = fields ? fields.join(", ") : "*";
    params.query = `SELECT ${fields}`;

    /**
     * для указания названия таблицы
     * @param {String} tableName - название таблицы
     * @returns {Function} с where, orWhere, build
     */
    const from = (tableName) => {
      if (!tableName) {
        throw new Error("Table name required");
      }

      params.query += ` FROM ${tableName}`;

      return { where, orWhere, build };
    };

    /**
     * для подготовки секции where с AND
     * @param {Array} whereParams - с запросами для секции where
     * @returns {Function} с orWhere, orderBy, build
     */
    const where = (whereParams = []) => {
      prepareWhereSection(whereParams);

      return { orWhere, orderBy, build };
    };

    /**
     * для подготовки секции where с OR
     * @param {Array} whereParams - с запросами для секции where
     * @returns {Function} с where, orderBy, build
     */
    const orWhere = (whereParams = []) => {
      prepareWhereSection(whereParams, "OR");

      return { where, orderBy, build };
    };

    /**
     * для сортировки результата запроса по возрастанию по столбцу
     * @param {String} value - название столбца
     * @returns {Function} с limit, build
     */
    const orderBy = (value) => {
      params.orderBy = `ORDER BY ${value}`;

      return { limit, build };
    };

    /**
     * для установки количества возвращаемых строк, максимум 1000
     * @param {String|Number} value - количество строк
     * @returns {Function} с offset, build
     */
    const limit = (value) => {
      params.limit = `LIMIT ${value}`;

      return { offset, build };
    };

    /**
     * для установки отступа от начала (в строках)
     * @param {String|Number} value - количество строк
     * @returns {Function} с build
     */
    const offset = (value) => {
      if (value) {
        params.offset = `OFFSET ${value}`;
      }

      return { build };
    };

    /**
     * подготовка секции where для select
     * @param {Array} whereParams - с запросами для секции where
     * @param {String} [logicalOperator = 'AND'] - применяемый в секции where
     */
    const prepareWhereSection = (whereParams = [], logicalOperator = "AND") => {
      if (whereParams.length !== 0) {
        if (!params.where) {
          params.where = "WHERE ";
        } else {
          params.where += ` ${logicalOperator} `;
        }

        whereParams.forEach((item, index) => {
          switch (index) {
            case 0:
              params.where += `(${item})`;
              break;

            default:
              params.where += ` ${logicalOperator} (${item})`;
              break;
          }
        });
      }
    };

    /**
     * для построения строки запроса
     * @returns {String} строку запроса
     */
    const build = () => {
      console.log("params :>> ", params);

      if (params.where) {
        params.query += " " + params.where;
      }

      if (params.orderBy) {
        params.query += " " + params.orderBy;
      }

      if (params.limit) {
        params.query += " " + params.limit;
      }

      if (params.offset) {
        params.query += " " + params.offset;
      }

      return params.query + ";";
    };

    return { from };
  };

  /**
   * для сохранения или обновления одной строки
   * @param {Array} data - данные для записи, обязательно должны содержать первичный ключ
   * @returns {Function} с into
   */
  const upsert = (data) => {
    const params = {};
    const fields = "(" + Object.keys(data).join(", ") + ")";
    const values = Object.values(data)
      .map((item) => {
        if (item === null) {
          return "null";
        } else if (typeof item === "string") {
          return `'${item}'`;
        } else {
          return `${item}`;
        }
      })
      .join(", ");

    params.query = `${fields} VALUES (${values})`;

    /**
     * для указания названия таблицы
     * @param {String} tableName - название таблицы
     * @returns {Function} с build
     */
    const into = (tableName) => {
      if (!tableName) {
        throw new Error("Table name required");
      }

      params.query = `UPSERT INTO ${tableName} ${params.query}`;

      return { build };
    };

    /**
     * для построения строки запроса
     * @returns {String} строку запроса
     */
    const build = () => {
      return params.query + ";";
    };

    return { into };
  };

  /**
   * для множественного сохранения или обновления строк, максимальный размер данных 2,5 Мб???
   * @param {Array} struct - с описанием записываемой структуры данных
   * @param {String} struct[0] - название переменной в формате $название
   * @param {Object} struct[1] - с описанием полей и их типов
   * @returns {Function} функцию into для указания названия таблицы
   */
  const bulkUpsert = (struct) => {
    const params = {};

    /**
     * для указания названия таблицы
     * @param {String} tableName - название таблицы
     * @returns {Function} с build
     */
    const into = (tableName) => {
      if (!tableName) {
        throw new Error("Table name required");
      }

      params.tableName = tableName;

      return { build };
    };

    /**
     * для построения строки запроса
     * @returns {String} строку запроса
     */
    const build = () => {
      const declare = (struct) => {
        const [name, item] = struct;

        let result = `DECLARE ${name} AS `;

        result += "List<Struct<\n";
        result += Object.entries(item)
          .map(([key, value]) => `${key}: ${value}`)
          .join(",\n");
        result += ">>";

        result += ";\n";

        return result;
      };

      const upsertInto = (tableName, struct) => {
        const [name, item] = struct;
        let result = `UPSERT INTO ${tableName}\nSELECT\n`;

        result += Object.keys(item).join(",\n");
        result += `\nFROM AS_TABLE(${name});`;

        return result;
      };

      return declare(struct) + "\n" + upsertInto(params.tableName, struct);
    };

    return { into };
  };

  const remove = () => {};

  return { select, upsert, bulkUpsert, remove };
};

module.exports = { queryBuilder };
