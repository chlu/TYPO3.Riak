<?php
namespace TYPO3\Riak\Tests\Unit\Cache;

/*                                                                        *
 * This script belongs to the TYPO3 Flow package "TYPO3.Riak".            *
 *                                                                        *
 *                                                                        */

/**
 * Unit test for RiakBackend
 */
class RiakBackendTest extends \TYPO3\Flow\Tests\UnitTestCase {

	/**
	 * @var \TYPO3\Riak\Cache\RiakBackend
	 */
	protected $backend;

	/**
	 * Initialize a Riak cache backend
	 */
	public function setUp() {
		$context = new \TYPO3\Flow\Core\ApplicationContext('Testing');
		$this->backend = new \TYPO3\Riak\Cache\RiakBackend($context, array(
			'hostname' => '127.0.0.1',
			'port' => 8091
		));
		$this->backend->initializeObject();
	}

	/**
	 * Flush the Riak backend after tests
	 */
	public function tearDown() {
		$this->backend->flush();
	}

	/**
	 * @test
	 */
	public function setAndGetEntry() {
		$this->backend->set('Foo', 'Bar');

		$this->assertEquals('Bar', $this->backend->get('Foo'));
	}

	/**
	 * @test
	 */
	public function hasEntry() {
		$this->backend->set('Foo', 'Bar');

		$this->assertFalse($this->backend->get('Unknown'));

		$this->assertTrue($this->backend->has('Foo'));
	}

	/**
	 * @test
	 */
	public function removeEntry() {
		$this->backend->set('Foo', 'Bar');

		$this->assertTrue($this->backend->remove('Foo'));

		$this->assertFalse($this->backend->has('Foo'));
	}

	/**
	 * @test
	 */
	public function findIdentifiersByTag() {
		$this->backend->set('X1', 'Bar', array('Session_1'));
		$this->backend->set('X2', 'Baz', array('Session_1'));

		$identifiers = $this->backend->findIdentifiersByTag('Session_1');
		sort($identifiers);
		$this->assertEquals(array('X1', 'X2'), $identifiers);
	}

	/**
	 * @test
	 */
	public function flushBackend() {
		$this->backend->set('Foo', 'Bar');

		$this->backend->flush();

		$this->assertFalse($this->backend->has('Foo'));
	}

	/**
	 * @test
	 */
	public function collectGarbageDeletesExpiredEntries() {
		$this->backend->set('Gone', 'with the wind', array(), 0);

		usleep(100000);

		$this->backend->collectGarbage();

		$this->assertFalse($this->backend->has('Gone'));
	}

}
?>