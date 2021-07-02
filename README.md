BigQuery Driver for Cakephp3
========

An BigQuery for CakePHP 3.5,3.6,3.7

## Installing via composer

Install [composer](http://getcomposer.org) and run:

```bash
composer require giginc/cakephp3-driver-bigquery
```

## Defining a connection
Now, you need to set the connection in your config/app.php file:

```php
 'Datasources' => [
...
    'bigquery' => [
        'className' => 'Giginc\BigQuery\Database\Connection',
        'driver' => 'Giginc\BigQuery\Database\Driver\BigQuery',
        'projectId' => env('BIGQUERY_PROJECT_ID', 'project_id'),
        'dataSet' => env('BIGQUERY_DATASET', 'dataset'),
        'keyFile' => [], // Console. Ex: json_decode(file_get_contents($path), true).
        'keyFilePath' => null, //The full path to your service account credentials .json file retrieved.
        'requestTimeout' => 0, // Defaults to 0 with REST and 60 with gRPC.
        'retries' => 3, // Number of retries for a failed request. Defaults to 3.
        'location' => 'us', // If provided, determines the default geographic location used when creating datasets and managing jobs.
        'maximumBytesBilled' => 1000000,
    ],

],
```

## Models
After that, you need to load Giginc\BigQuery\ORM\Table in your tables class:

### Table
```php
//src/Model/Table/ProductsTable.php
namespace App\Model\Table;

use Giginc\BigQuery\ORM\Table;

/**
 * ProductsTable Table
 *
 * @uses Table
 * @package Table
 */
class ProductsTable extends Table
{
    public function initialize(array $config)
    {
        parent::initialize($config);

        $this->setTable('products_%Y%m%d');
    }

    public static function defaultConnectionName()
    {
        return 'bigquery';
    }

    public function findOk($query, array $options)
    {
        $query = $query
            ->where([
                'status' => 'ok',
            ]);

        return $query;
    }
}
```

### Entity
```php
//src/Model/Entity/Product.php
namespace App\Model\Entity;

use Cake\ORM\Entity;

/**
 * Product Entity
 *
 * @uses Entity
 * @package Entity
 */
class Product extends Entity
{
    protected $_accessible = [
        '*' => true,
        'id' => false,
    ];

    protected $_virtual = [
    ];
}

## Controllers

```php
namespace App\Controller;

use App\Controller\AppController;

/**
 * Pages Controller
 *
 * @property \App\Model\Table\PagesTable $Pages
 *
 * @method \App\Model\Entity\Review[]|\Cake\Datasource\ResultSetInterface paginate($object = null, array $settings = [])
 */
class PagesController extends AppController
{
    /**
     * Index method
     *
     * @access public
     * @return \Cake\Http\Response|void
     */
    public function index()
    {
        $this->loadModel('Products');
        $data = $this->Products->date('2021-04-12')
            ->find()
            ->fields([
                'name',
                'description',
                'MAX(hit_count) AS max',
                'COUNT(*) AS count',
            ])
            ->where([
                'name' => 'iphone',
                'hit_count >' => 0,
            ])
            ->group(['name'])
            ->order(['name' => 'DESC'])
            ->limit(5)
            ->all();
    }
}
```

## LICENSE

[The MIT License (MIT) Copyright (c) 2021](http://opensource.org/licenses/MIT)
