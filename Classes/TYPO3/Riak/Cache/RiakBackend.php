<?php
namespace TYPO3\Riak\Cache;

/*                                                                        *
 * This script belongs to the TYPO3 Flow package "TYPO3.Riak".            *
 *                                                                        *
 *                                                                        */

/**
 * A Riak cache backend
 */
class RiakBackend extends \TYPO3\Flow\Cache\Backend\AbstractBackend implements \TYPO3\Flow\Cache\Backend\TaggableBackendInterface {

	/**
	 * Instance of the Riak client
	 * @var \Riak\Client
	 */
	protected $client;

	/**
	 * @var \Riak\Bucket
	 */
	protected $bucket;

	/**
	 * Indicates whether the server is connected
	 * @var boolean
	 */
	protected $connected = FALSE;

	/**
	 * Hostname / IP of the Riak server, defaults to 127.0.0.1.
	 * @var string
	 */
	protected $hostname = '127.0.0.1';

	/**
	 * Port of the Riak server, defaults to 8098
	 * @var integer
	 */
	protected $port = 8098;

	/**
	 * Bucket for storing cache keys
	 * @var string
	 */
	protected $bucketName = 'TYPO3_Flow';

	/**
	 * Initializes the Riak client
	 *
	 * @return void
	 */
	public function initializeObject() {
		$this->client = new \Riak\Client($this->hostname, $this->port);
		$this->bucket = $this->client->bucket($this->bucketName);
	}

	/**
	 * Setter for server hostname
	 *
	 * @param string $hostname Hostname
	 * @return void
	 */
	public function setHostname($hostname) {
		$this->hostname = $hostname;
	}

	/**
	 * Setter for server port
	 *
	 * @param integer $port Port
	 * @return void
	 */
	public function setPort($port) {
		$this->port = $port;
	}

	/**
	 * @param string $bucketName
	 */
	public function setBucketName($bucketName) {
		$this->bucketName = $bucketName;
	}

	/**
	 * Save data in the cache
	 *
	 *
	 * @param string $entryIdentifier Identifier for this specific cache entry
	 * @param string $data Data to be stored
	 * @param array $tags Tags to associate with this cache entry
	 * @param integer $lifetime Lifetime of this cache entry in seconds. If NULL is specified, default lifetime is used. "0" means unlimited lifetime.
	 * @return void
	 * @throws \InvalidArgumentException if identifier is not valid
	 * @throws \TYPO3\Flow\Cache\Exception\InvalidDataException if data is not a string
	 */
	public function set($entryIdentifier, $data, array $tags = array(), $lifetime = NULL) {
		if (!is_string($entryIdentifier)) {
			throw new \InvalidArgumentException('The specified identifier is of type "' . gettype($entryIdentifier) . '" but a string is expected.', 1279470252);
		}
		if (!is_string($data)) {
			throw new \TYPO3\Flow\Cache\Exception\InvalidDataException('The specified data is of type "' . gettype($data) . '" but a string is expected.', 1279469941);
		}

		$entry = $this->bucket->getBinary($entryIdentifier);
		if ($entry->exists()) {
			$entry->setData($data);
		} else {
			$entry = $this->bucket->newBinary($entryIdentifier, $data, 'application/x-flow-cache');
		}
		if ($lifetime !== NULL) {
			$entry->addIndex('expiration', 'int', time() + $lifetime);
		} else {
			$entry->removeIndex('expiration');
		}
			// Marker index to allow more efficient flush through 2i
		$entry->addIndex('cache', 'int', 1);
		foreach ($tags as $tag) {
			$entry->addIndex('tag', 'bin', $tag);
		}
		$entry->store();
	}

	/**
	 * Loads data from the cache.
	 *
	 * @param string $entryIdentifier An identifier which describes the cache entry to load
	 * @return mixed The cache entry's content as a string or FALSE if the cache entry could not be loaded
	 * @throws \InvalidArgumentException if identifier is not a string
	 */
	public function get($entryIdentifier) {
		if (!is_string($entryIdentifier)) {
			throw new \InvalidArgumentException('The specified identifier is of type "' . gettype($entryIdentifier) . '" but a string is expected.', 1279470253);
		}

		$entry = $this->bucket->getBinary($entryIdentifier);
		if (!$entry->exists()) {
			return FALSE;
		}

			// Explicitly expire entries
		if ($entry->getIndex('expiration') !== array()) {
			list($expiration) = (integer)$entry->getIndex('expiration');
			if ($expiration < time()) {
				$entry->delete();
				return FALSE;
			}
		}

		return $entry->getData();
	}

	/**
	 * Checks if a cache entry with the specified identifier exists.
	 *
	 * @param string $entryIdentifier Identifier specifying the cache entry
	 * @return boolean TRUE if such an entry exists, FALSE if not
	 * @throws \InvalidArgumentException if identifier is not a string
	 */
	public function has($entryIdentifier) {
		if (!is_string($entryIdentifier)) {
			throw new \InvalidArgumentException('The specified identifier is of type "' . gettype($entryIdentifier) . '" but a string is expected.', 1279470254);
		}
		return $this->bucket->getBinary($entryIdentifier)->exists();
	}

	/**
	 * Removes all cache entries matching the specified identifier.
	 *
	 * @param string $entryIdentifier Specifies the cache entry to remove
	 * @return boolean TRUE if (at least) an entry could be removed or FALSE if no entry was found
	 * @throws \InvalidArgumentException if identifier is not a string
	 */
	public function remove($entryIdentifier) {
		if (!is_string($entryIdentifier)) {
			throw new \InvalidArgumentException('The specified identifier is of type "' . gettype($entryIdentifier) . '" but a string is expected.', 1279470255);
		}

		$entry = $this->bucket->getBinary($entryIdentifier);
		if (!$entry->exists()) {
			return FALSE;
		}
		$entry->delete();
		return TRUE;
	}

	/**
	 * Finds and returns all cache entry identifiers which are tagged by the
	 * specified tag.
	 *
	 * @param string $tag The tag to search for
	 * @return array An array of entries with all matching entries. An empty array if no entries matched
	 * @throws \InvalidArgumentException if tag is not a string
	 */
	public function findIdentifiersByTag($tag) {
		if (!is_string($tag)) {
			throw new \InvalidArgumentException('The specified tag is of type "' . gettype($tag) . '" but a string is expected.', 1279569759);
		}

			// Needs LevelDB backend in Riak!
			// TODO Handle "indexes_not_supported" error
		$keys = $this->bucket->indexSearch('tag', 'bin', $tag, NULL, TRUE);

		return array_map(function($link) { return $link->getKey(); }, $keys);
	}

	/**
	 * Removes all cache entries of this cache.
	 *
	 * @return void
	 */
	public function flush() {
		$keys = $this->bucket->indexSearch('cache', 'int', 1);
		foreach ($keys as $link) {
			$link->getBinary()->delete();
		}
	}

	/**
	 * Removes all cache entries of this cache which are tagged with the specified tag.
	 *
	 * @param string $tag Tag the entries must have
	 * @return void
	 * @throws \InvalidArgumentException if identifier is not a string
	 */
	public function flushByTag($tag) {
		if (!is_string($tag)) {
			throw new \InvalidArgumentException('The specified tag is of type "' . gettype($tag) . '" but a string is expected.', 1279578078);
		}

		$keys = $this->bucket->indexSearch('tag', 'bin', $tag);
		foreach ($keys as $link) {
			$link->getBinary()->delete();
		}
	}

	/**
	 * Delete expired keys
	 *
	 * @return void
	 */
	public function collectGarbage() {
		$keys = $this->bucket->indexSearch('expiration', 'int', 0, time());
		foreach ($keys as $link) {
			$link->getBinary()->delete();
		}
	}

}
?>