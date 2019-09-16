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

                $phpdoc = $method->getDocComment();
                if ($phpdoc !== false && ($providerPos = strpos($phpdoc, '@dataProvider')) !== false) {
                    $providerMethodPos = $providerPos + 14;
                    $providerMethodLen = strpos($phpdoc, "\n", $providerMethodPos) - $providerMethodPos;
                    $providerMethod = substr($phpdoc, $providerMethodPos, $providerMethodLen);

                    $testCase = new $className($this->provider->getRedisClient());
                    foreach ($testCase->$providerMethod() as $args) {
                        $testCase = new $className($this->provider->getRedisClient());
                        call_user_func_array([$testCase, $methodName], (array)$args);
                    }
                } else {
                    if ($className === 'PhpRQ\\BaseTest') {
                        $testCase = new $className();
                    } else {
                        $testCase = new $className($this->provider->getRedisClient());
                    }

                    $testCase->$methodName();
                }
            }
        }
    }

}
