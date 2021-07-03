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
     * @var \Giginc\BigQuery\Database
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
        'keyFile' => [], // json
        'keyFilePath' => null,
        'requestTimeout' => 0,
        'retries' => 3,
        'location' => '',
        'maximumBytesBilled' => 1000000,
    ];

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
     * @param string $key key
     * @return array
     * @access public
     */
    public function getConfig(string $key = '')
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
            $config = [
                'projectId' => $this->_config['projectId'],
                'requestTimeout' => $this->_config['requestTimeout'],
                'retries' => $this->_config['retries'],
            ];

            // config keyFile
            if ($this->_config['keyFile']) {
                $config['keyFile'] = $this->_config['keyFile'];
            }
            // config keyFilePath
            if ($this->_config['keyFilePath']) {
                $config['keyFilePath'] = $this->_config['keyFilePath'];
            }
            // config location
            if ($this->_config['location']) {
                $config['location'] = $this->_config['location'];
            }

            $this->_db = new BigQueryClient($config);

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
