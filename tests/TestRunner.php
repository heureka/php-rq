<?php

namespace PhpRQ;

use Nette\Utils\Finder;

/**
 * @author Jakub Chábek <jakub.chabek@heureka.cz>
 */
class TestRunner
{

    /**
     * @var ClientProvider
     */
    private $provider;

    public function __construct(ClientProvider $provider)
    {
        $this->provider = $provider;
    }

    public function run()
    {
        foreach (Finder::findFiles('*Test.php')->from(__DIR__) as $fileInfo) {
            /** @var \SplFileInfo $fileInfo*/

            $baseName = $fileInfo->getBasename('.php');
            if ($baseName === 'PhpRQTest') {
                continue;
            }

            $className = 'PhpRQ\\' . $baseName;

            $reflection = new \ReflectionClass($className);
            if (!$reflection->isInstantiable()) {
                continue;
            }

            foreach ($reflection->getMethods() as $method) {
                if (!$method->isPublic() || strpos($methodName = $method->getName(), 'test') === false) {
                    continue;
                }

                $testCase = new $className($this->provider->getRedisClient());
                $testCase->$methodName();
            }
        }
    }

}
