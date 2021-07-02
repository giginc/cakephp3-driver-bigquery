<?php
declare(strict_types=1);

namespace Giginc\BigQuery\Database\Driver;

use Exception;
use Google\Cloud\BigQuery\BigQueryClient;

/**
 * BigQuery
 *
 * @copyright Copyright (c) 2021,GIG inc.
 * @author Shota KAGAWA <kagawa@giginc.co.jp>
 */
class BigQuery
{
    /**
     * Config
     *
     * @var array
     * @access private
     */
    private $_config;

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
     * Database Instance
     *
     * @var Giginc\BigQuery\Database
     * @access protected
     */
    protected $_db = null;

    /**
     * Base Config
     *
     * @var array
     * @access public
     *
     */
    protected $_baseConfig = [
        'projectId' => null,
        'dataSet' => null,
        'requestTimeout' => 0,
        'retries' => 3,
        'maximumBytesBilled' => 1,
    ];

    /**
     * Direct connection with database
     *
     * @var mixed null | BigQuery
     * @access private
     */
    private $connection = null;

    /**
     * @param array $config configuration
     */
    public function __construct($config)
    {
        $this->_config = array_merge($this->_baseConfig, $config);
    }

    /**
     * return configuration
     *
     * @return array
     * @access public
     */
    public function getConfig(string $key=null)
    {
        if ($key) {
            return $this->_config[$key];
        } else {
            return $this->_config;
        }
    }

    /**
     * connect to the database
     *
     * @param string $name BigQuery file name.
     * @access public
     * @return bool
     */
    public function connect(string $name)
    {
        try {
            $this->_db = new BigQueryClient([
                'projectId' => $this->_config['projectId'],
                'requestTimeout' => $this->_config['requestTimeout'],
                'retries' => $this->_config['retries'],
            ]);

            $this->connected = true;
        } catch (Exception $e) {
            trigger_error($e->getMessage());
        }

        return $this->connected;
    }

    /**
     * return database connection
     *
     * @param string $name Csv file name.
     * @access public
     * @return \Giginc\Csv\Database\Driver\File
     */
    public function getConnection($name)
    {
        if (!$this->isConnected()) {
            $this->connect($name);
        }

        return $this->_db;
    }

    /**
     * disconnect from the database
     *
     * @return bool
     * @access public
     */
    public function disconnect()
    {
        if ($this->connected) {
            return $this->connected = false;
        }

        return true;
    }

    /**
     * database connection status
     *
     * @return bool
     * @access public
     */
    public function isConnected()
    {
        return $this->connected;
    }

    /**
     * @return bool
     */
    public function enabled()
    {
        return true;
    }
}
