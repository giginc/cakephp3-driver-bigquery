<?php
declare(strict_types=1);

namespace Giginc\BigQuery\ORM;

use BadMethodCallException;
use Cake\Datasource\EntityInterface;
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
        $this->_driver = $this->getConnection()->getDriver();

        if (!$this->_driver instanceof BigQuery) {
            throw new Exception("Driver must be an instance of 'Giginc\BigQuery\Database\Driver\BigQuery'");
        }
        $this->_db = $this->_driver->getConnection($this->getTable());

        $this->getSchema();

        return $this->_db;
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

 //           $this->_schema = $this->_db->getSchema();
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
     * getTableName
     *
     * @access public
     * @return void
     */
    public function getTableName()
    {
       preg_match_all("/%(.)/", $this->_table, $matchs);
       $dateString = implode('', $matchs[0]);
       $dateFormat = implode('', $matchs[1]);

       $date = "*";
       if ($this->_date) {
           $date = new DateTime($this->_date);
           $date = $date->format($dateFormat);
       }

       return str_replace($dateString, $date, $this->_table);
    }

    /**
     * select
     *
     * @access private
     * @return void
     */
    private function select()
    {
        $connection = $this->_getConnection();

        $fields =  "*";
        if ($this->_fields) {
            $fields = implode(', ', $this->_fields);
        }

        $dataSet = $this->_driver->getConfig('dataSet');
        $tableName = $this->getTableName();

        $query = "SELECT {$fields} FROM `{$dataSet}.{$tableName}`";

        return $query;
    }

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
     * @param array $array 
     * @access public
     * @return void
     */
    public function fields(array $array)
    {
        $this->_fields = $array;

        return $this;
    }

    /**
     * between
     *
     * @param array $array 
     * @access public
     * @return void
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
     * @param array $array 
     * @access public
     * @return void
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
     * @param array $array 
     * @access public
     * @return void
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
     * @param array $array 
     * @access public
     * @return void
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
     * @param int $number 
     * @access public
     * @return void
     */
    public function limit(int $number)
    {
        $this->_limit = $number;

        return $this;
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

        return $response;
    }

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
     * all
     *
     * @access public
     * @return void
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
}
