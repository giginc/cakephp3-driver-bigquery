<?php
declare(strict_types=1);

namespace Giginc\BigQuery\ORM;

use BadMethodCallException;
use Cake\ORM\Table as CakeTable;
use DateTime;
use Exception;
use Giginc\BigQuery\Database\Driver\BigQuery;

class Table extends CakeTable
{
    protected $_driver;

    protected $_db;

    protected $_job;

    private $_date;
    private $_query;
    private $_parameters;
    private $_fields;
    private $_where;
    private $_group;
    private $_order;
    private $_limit;

    /**
     * Are we connected to the DataSource?
     *
     * true - yes
     * false - nope, and we can't connect
     *
     * @var bool
     * @access public
     */
    public $connected = false;

    /**
     * The schema object containing a description of this table fields
     *
     * @var \Cake\Database\Schema\TableSchema
     */
    protected $_schema;

    /**
     * return BigQuery
     *
     * @return \Giginc\BigQuery\ORM\file
     * @throws \Exception
     */
    private function _getConnection()
    {
        if ($this->connected === false) {
            $this->_driver = $this->getConnection()->getDriver();

            if (!$this->_driver instanceof BigQuery) {
                throw new Exception("Driver must be an instance of 'Giginc\BigQuery\Database\Driver\BigQuery'");
            }
            $this->_db = $this->_driver->getConnection($this->getTable());

            $this->connected = true;
        }

        return $this->_db;
    }

    /**
     * select
     *
     * @access private
     * @return string
     */
    private function select()
    {
        $connection = $this->_getConnection();

        $fields = "*";
        if ($this->_fields) {
            $fields = implode(', ', $this->_fields);
        }

        $dataSet = $this->_driver->getConfig('dataSet');
        $tableId = $this->getTableId();

        $query = "SELECT {$fields} FROM `{$dataSet}.{$tableId}`";

        return $query;
    }

    /**
     * getResponse
     *
     * @param \Cake\ORM\Query $queryResult Query Result
     * @access private
     * @return array
     */
    private function getResponse($queryResult)
    {
        $response = [];
        $entity = $this->getEntityClass();
        foreach ($queryResult as $record) {
             $response[] = new $entity($record);
        }

        return $response;
    }

    /**
     * Returns the schema table object describing this table's properties.
     *
     * @access public
     * @return \Cake\Database\Schema\TableSchema
     */
    public function getSchema()
    {
        if ($this->_schema === null) {
            // search last table
            $datasetId = $this->_driver->getConfig('dataSet');
            $dataset = $this->_db->dataset($datasetId);
            $lastTableId = $this->getLastTableId();
            $table = $dataset->table($lastTableId);
            $tableInfo = $table->info();

            return $tableInfo['schema'];
        }

        return $this->_schema;
    }

    /**
     * Sets the schema table object describing this table's properties.
     *
     * If an array is passed, a new TableSchema will be constructed
     * out of it and used as the schema for this table.
     *
     * @param array|\Cake\Database\Schema\TableSchema $schema Schema to be used for this table
     * @return $this
     */
    public function setSchema($schema)
    {
        if (is_array($schema)) {
            $this->_schema = $schema;
        }

        return $this;
    }

    /**
     * Closes the current datasource connection.
     *
     * @access public
     * @return void
     */
    public function disconnect()
    {
        $this->_driver->disconnect();
    }

    /**
     * client
     *
     * @access public
     * @return array
     */
    public function client()
    {
        $connection = $this->_getConnection();

        return $this->_db;
    }

    /**
     * getTableId
     *
     * @param string $default Default value
     * @access public
     * @return string
     */
    public function getTableId($default = '*')
    {
        preg_match_all("/%(.)/", $this->_table, $matchs);
        $dateString = implode('', $matchs[0]);
        $dateFormat = implode('', $matchs[1]);

        $date = $default;
        if ($this->_date) {
            $date = new DateTime($this->_date);
            $date = $date->format($dateFormat);
        }

        return str_replace($dateString, $date, $this->_table);
    }

    /**
     * getLastTableId
     *
     * @access public
     * @return string
     */
    public function getLastTableId()
    {
        $connection = $this->_getConnection();

        $datasetId = $this->_driver->getConfig('dataSet');
        $dataset = $this->_db->dataset($datasetId);
        $tables = $dataset->tables();
        $regexTableId = $this->getTableId('.*');

        $tableIds = [];
        foreach ($tables as $table) {
            if (preg_match("/^{$regexTableId}$/", $table->id())) {
                $tableIds[] = $table->id();
            }
        }
        sort($tableIds, SORT_STRING);

        return end($tableIds);
    }

    /**
     * copyTable
     * Uncomment and populate these variables in your code
     *
     * @param string $sourceTableId The BigQuery table ID to copy from
     * @param string $destinationTableId The BigQuery table ID to copy to
     * @access public
     * @return array
     */
    public function copyTable(string $sourceTableId, string $destinationTableId)
    {
        $connection = $this->_getConnection();

        $datasetId = $this->_driver->getConfig('dataSet');
        $dataset = $this->_db->dataset($datasetId);
        $sourceTable = $dataset->table($sourceTableId);
        $destinationTable = $dataset->table($destinationTableId);
        $copyConfig = $sourceTable->copy($destinationTable);

        return $sourceTable->runJob($copyConfig);
    }

    /**
     * copyTableSchema
     * Uncomment and populate these variables in your code
     *
     * @param string $sourceTableId The BigQuery table ID to copy from
     * @param string $destinationTableId The BigQuery table ID to copy to
     * @access public
     * @return array
     */
    public function copyTableSchema(string $sourceTableId, string $destinationTableId)
    {
        $connection = $this->_getConnection();

        $datasetId = $this->_driver->getConfig('dataSet');
        $dataset = $this->_db->dataset($datasetId);
        $sourceTable = $dataset->table($sourceTableId);
        $sourceTableInfo = $sourceTable->info();
        $sourceTableSchema = $sourceTableInfo['schema'];

        return $dataset->createTable($destinationTableId, [
            'schema' => $sourceTableSchema,
        ]);
    }

    /**
     * createTable
     * Uncomment and populate these variables in your code
     *
     * @param string $tableId The BigQuery table ID
     * @param array $fields The BigQuery table fields
     * @access public
     * @return array
     */
    public function createTable(string $tableId, array $fields)
    {
        $connection = $this->_getConnection();

        $datasetId = $this->_driver->getConfig('dataSet');
        $dataset = $this->_db->dataset($datasetId);
        $schema = ['fields' => $fields];

        return $dataset->createTable($tableId, ['schema' => $schema]);
    }

    /**
     * Creates a new Query instance for a table.
     *
     * @access public
     * @return \Cake\ORM\Query
     */
    public function query()
    {
        $options = [
            'maximumBytesBilled' => $this->_driver->getConfig('maximumBytesBilled'),
        ];

        $this->_job = $this->_db->query($this->_query, $options);

        if (!empty($this->_parameters)) {
            $this->_job = $this->_job->parameters($this->_parameters);
        }

        $response = $this->getResponse($this->_db->runQuery($this->_job));

        $this->disconnect();

        return $response;
    }

    /**
     * date
     *
     * @param string $string Date string
     * @access public
     * @return \Cake\ORM\Table
     */
    public function date(string $string = '')
    {
        $this->_date = $string;

        return $this;
    }

    /**
     * find documents
     *
     * @param string $type Type.
     * @param array $options Option.
     * @access public
     * @return array
     * @throws \Exception
     */
    public function find($type = 'all', $options = [])
    {
        if ($type == 'all') {
        } else {
            $finder = 'find' . ucfirst($type);
            if (method_exists($this, $finder)) {
                $this->{$finder}($query, $options);
            } else {
                throw new BadMethodCallException(
                    sprintf('Unknown finder method "%s"', $type)
                );
            }
        }

        return $this;
    }

    /**
     * fields
     *
     * @param array $array Array
     * @access public
     * @return \Cake\ORM\Table
     */
    public function fields(array $array)
    {
        $this->_fields = $array;

        return $this;
    }

    /**
     * between
     *
     * @param array $array Array
     * @access public
     * @return \Cake\ORM\Table
     */
    public function between(array $array)
    {
        $i = 0;
        foreach ($array as $key => $value) {
            $this->_where[] = " {$key} BETWEEN @between{$i}0 AND @between{$i}1";
            $this->_parameters["between{$i}0"] = $value[0];
            $this->_parameters["between{$i}1"] = $value[1];
            $i++;
        }

        return $this;
    }

    /**
     * where
     *
     * @param array $array Array
     * @access public
     * @return \Cake\ORM\Table
     */
    public function where(array $array)
    {
        $i = 0;
        foreach ($array as $key => $value) {
            if (is_null($value)) {
                $this->_where[] = " {$key} NULL";
            } else {
                $this->_where[] = " {$key} @where{$i}";
                $this->_parameters["where{$i}"] = $value;
            }
            $i++;
        }

        return $this;
    }

    /**
     * group
     *
     * @param array $array Array
     * @access public
     * @return \Cake\ORM\Table
     */
    public function group(array $array)
    {
        $group = [];
        foreach ($array as $row) {
            $group[] = "`{$row}`";
        }
        $this->_group = $group;

        return $this;
    }

    /**
     * order
     *
     * @param array $array Array
     * @access public
     * @return \Cake\ORM\Table
     */
    public function order(array $array)
    {
        $order = [];
        foreach ($array as $key => $value) {
            // default order
            if (!$value) {
                $value = "ASC";
            }

            if (preg_match("/^(ASC|DESC)$/i", $value)) {
                $order[] = "`{$key}` {$value}";
            }
        }

        $this->_order = $order;

        return $this;
    }

    /**
     * limit
     *
     * @param int $number Limit number
     * @access public
     * @return \Cake\ORM\Table
     */
    public function limit(int $number)
    {
        $this->_limit = $number;

        return $this;
    }

    /**
     * all
     *
     * @access public
     * @return \Cake\ORM\Table
     */
    public function all()
    {
        $this->_query = $this->select();

        // where
        if ($this->_where) {
            $this->_query .= " WHERE ";

            $this->_query .= implode(" AND", $this->_where);
        }

        // group
        if ($this->_group) {
            $this->_query .= " GROUP BY ";

            $this->_query .= implode(", ", $this->_group);
        }

        // order
        if ($this->_order) {
            $this->_query .= " ORDER BY ";

            $this->_query .= implode(", ", $this->_order);
        }

        // limit
        if ($this->_limit) {
            $this->_query .= " LIMIT " . (int)$this->_limit;
        }

        return $this->query();
    }

    /**
     * insert
     * instantiate the bigquery table service
     *
     * @param array $data Data
     * @access public
     * @return bool
     */
    public function insert(array $data)
    {
        $connection = $this->_getConnection();

        $datasetId = $this->_driver->getConfig('dataSet');
        $tableId = $this->getTableId(date('Ymd'));

        $dataset = $this->_db->dataset($datasetId);
        $table = $dataset->table($tableId);

        // not exists table
        if (!$table->exists()) {
            $this->createTable($tableId, $this->getSchema());
        }

        $response = $table->insertRows([[
            'insertId' => date('YmdHisu'), // exclusion control
            'data' => $data,
        ]]);

        return $response->isSuccessful();
    }
}
