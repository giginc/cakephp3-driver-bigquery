<?php
declare(strict_types=1);

namespace Giginc\BigQuery\Database;

use Cake\Database\Exception\MissingConnectionException;
use Giginc\BigQuery\Database\Driver\BigQuery;

class Connection extends \Cake\Database\Connection
{

    /**
     * Contains the configuration param for this connection
     *
     * @var array
     */
    protected $_config;

    /**
     * Database Driver object
     *
     * @var \Giginc\Mongodb\Database\Driver\Mongodb;
     */
    protected $_driver = null;

    /**
     * MongoSchema
     *
     * @var MongoSchema
     * @access protected
     */
    protected $_schemaCollection;

    /**
     * disconnect existent connection
     *
     * @access public
     * @return void
     */
    public function __destruct()
    {
        if ($this->_driver->connected) {
            $this->_driver->disconnect();
            unset($this->_driver);
        }
    }

    /**
     * return configuration
     *
     * @return array $_config
     * @access public
     */
    public function config()
    {
        return $this->_config;
    }

    /**
     * return configuration name
     *
     * @return string
     * @access public
     */
    public function configName()
    {
        return 'bigquery';
    }

    /**
     * @param null $driver
     * @param array $config
     * @return  BigQuery
     */
    public function driver($driver = null, $config = [])
    {
        if ($driver === null) {
            return $this->_driver;
        }
        $this->_driver = new BigQuery($config);

        return $this->_driver;
    }

    /**
     * connect to the database
     *
     * @return boolean
     * @access public
     */
    public function connect()
    {
        try {
            $this->_driver->connect();
            return true;
        } catch (\Exception $e) {
            throw new MissingConnectionException(['reason' => $e->getMessage()]);
        }
    }

    /**
     * disconnect from the database
     *
     * @return boolean
     * @access public
     */
    public function disconnect()
    {
        if ($this->_driver->isConnected()) {
            return $this->_driver->disconnect();
        }

        return true;
    }
}
